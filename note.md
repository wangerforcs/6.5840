## MapReduce

### 流程
1. 将输入的大文件拆分为M个Split
2. 整个计算过程包括M个Map任务和N个Reduce任务，由Master分配给Worker
3. Map任务需要读入文件，使用Mapf函数将文件内容内容解析为键值对，并将每个键值对结果保存在hash(key) mod N对应的文件中(本地磁盘)，任务完成后向Master报告，并约定文件的存放位置或者将这些数据移动到Reduce所需要的位置
4. Master在收到Map完成消息后会分配Reduce任务给Woker
5. Reduce任务需要读入Map任务的输出文件，使用Reducef函数将相同key的value合并(先排序再合并)，最后将结果写入新的Reduce文件(GFS共享文件)，任务完成后向Master报告

单词计数的Map和Reduce函数
```go
func Map(filename string, contents string) []mr.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
```
Master会记录每个Map或Reduce任务，以及对应的Worker，如果Worker挂掉，Master会重新分配任务

### 优化机制
MapReduce的实现依赖于GFS提供的分布式原子读写操作。当MapReduce的Master节点拆分Map任务并分包到不同的worker服务器上时，Master节点会找出输入文件具体存在哪台GFS服务器上，并把对应于那个输入文件的Map Task调度到同一台服务器上。几乎所有的Map函数都会运行在存储了数据的相同机器上，并因此节省了大量的时间，否则通过网络来读取输入数据将会耗费大量的时间。但是，确实需要将每一份中间数据都通过网络从创建它的Map节点传输到需要它的Reduce节点。所以，这也是MapReduce中代价较大的一部分。
#### Worker故障
在 MapReduce 集群中，Master 会周期地向每一个 Worker 发送 Ping 信号。如果某个 Worker 在一段时间内没有响应，Master 就会认为这个 Worker 已经不可用。
任何分配给该 Worker 的 Map 任务，需要由 Master 重新分配给其他 Worker，同时通知给所有 Reducer，没能从原本的 Mapper 上完整获取中间结果的 Reducer 便会开始从新的 Mapper 上获取数据。
分配给该 Worker 的未完成的 Reduce 任务，Master 会重新分配给其他 Worker，已完成的 Reduce 任务的结果由GFS保证可用。
#### Master故障
Master 结点在运行时会周期性地将集群的当前状态作为保存点（Checkpoint）写入到磁盘中。Master 进程终止后，重新启动的 Master 进程即可利用存储在磁盘中的数据恢复到上一次保存点的状态。
#### 数据传输
在 Google 内部所使用的计算环境中，机器间的网络带宽是比较稀缺的资源，需要尽量减少在机器间过多地进行不必要的数据传输。

## GFS

### 分布式存储系统的特点

1. 容错 使用自动化的方法修复系统错误
2. 实现容错最有用的一种方法是使用复制，而复制带来了一致性问题。通过聪明的设计，你可以避免不一致的问题，并且让数据，看起来也表现的符合预期。但是达到这样的效果需要额外的工作，不同服务器之间通过网络额外的交互，会降低性能，与我们的要求不符。


### GFS的设计目标
大型的，快速的分布式文件系统
全局有效，而不是针对某个特定的应用程序构建特定的裁剪的存储系统

分割文件
每个包含了数据的文件会被GFS自动的分割并存放在多个服务器之上，这样读写操作自然就会变得很快。因为可以从多个服务器上同时读取同一个文件，进而获得更高的聚合吞吐量。将文件分割存储还可以在存储系统中保存比单个磁盘还要大的文件。

可用性
我们不希望每次服务器出了故障，派人到机房去修复服务器或者迁移数据，而要求系统能自动修复自己。

GFS在各个方面对大型的顺序文件读写做了定制，GFS是为TB级别的文件而生，并且GFS只会顺序处理，不支持随机访问。某种程度上来说，它有点像批处理的风格。GFS并没有花费过多的精力来降低延迟，它的关注点在于巨大的吞吐量上，所以单次操作都涉及到MB级别的数据。

### GFS结构
Master采用Active-Standby模式，只有一个Master在工作
Master节点用来管理文件和Chunk的信息，而Chunk服务器用来存储实际的数据，将这两类数据的管理问题几乎完全隔离开
Master节点知道每一个文件对应的所有的Chunk的ID，这些Chunk每个是64MB大小，它们共同构成了一个文件。如果我有一个1GB的文件，那么Master节点就知道文件的所有Chunk存放的位置。当我想读取这个文件中的任意一个部分时，我需要向Master节点查询对应的Chunk在哪个服务器上，之后我可以直接从Chunk服务器读取对应的Chunk数据。

需要记录的信息
1. 文件名到Chunk ID或者Chunk Handle数组的对应。这个表单告诉你，文件对应了哪些Chunk。
2. Chunk ID到Chunk数据的对应关系。包括：每个Chunk存储在哪些服务器上，所以这部分是Chunk服务器的列表；每个Chunk当前的版本号，所以Master节点必须记住每个Chunk对应的版本号。

所有对于Chunk的写操作都必须在主Chunk（Primary Chunk）上顺序处理，主Chunk是Chunk的多个副本之一。所以，Master节点必须记住哪个Chunk服务器持有主Chunk。并且，主Chunk只能在特定的租约时间内担任主Chunk，所以，Master节点要记住主Chunk的租约过期时间。

以上数据都存储在内存中，为了防止丢失Master节点会同时将数据存储在磁盘上。所以Master节点读数据只会从内存读，但是写数据的时候，有一部分数据会接入到磁盘中。更具体来说，Master会在磁盘上存储log，每次有数据变更时，Master会在磁盘的log中追加一条记录，并生成CheckPoint（类似于备份点）。

在磁盘中维护log而不是数据库的原因是，数据库本质上来说是某种B树或者hash table，相比之下，追加log会非常的高效，因为你可以将最近的多个log记录一次性的写入磁盘，这些数据都是向同一个地址追加，这样只需要等待磁盘的磁碟旋转一次。而对于B树来说，每一份数据都需要在磁盘中随机找个位置写入。

当Master节点故障重启，并重建它的状态，会从log中的最近一个checkpoint开始恢复，再逐条执行从Checkpoint开始的log，最后恢复自己的状态。

读文件
应用程序想读取某个特定文件的某个特定的偏移位置上的某段特定长度的数据，客户端（或者应用程序）将文件名和偏移量发送给Master，Master节点将Chunk Handle（也就是ID）和服务器列表发送给客户端。客户端会选择一个网络上最近的服务器，并将读请求发送到那个服务器。客户端会缓存Chunk和服务器的对应关系，这样，当再次读取相同Chunk数据时，就不用一次次的去向Master请求相同的信息。客户端与选出的Chunk服务器通信，将Chunk Handle和偏移量发送给那个Chunk服务器。Chunk服务器会在本地的硬盘上，将每个Chunk存储成独立的Linux文件，并通过普通的Linux文件系统管理。Chunk服务器需要做的就是根据文件名找到对应的Chunk文件，之后从文件中读取对应的数据段，并将数据返回给客户端。

对于读文件来说，可以从任何最新的Chunk副本读取数据，但是对于写文件来说，必须要通过Chunk的主副本（Primary Chunk）来写入。
