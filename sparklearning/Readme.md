1. RDD 的创建方式
    1. 从集合中创建
    2. 从外部存储中创建
    3. 从其他RDD创建
2. distinct 算子会打乱重组, shuffle
3. 一个分区的数据被打乱重组到其他分区, 称之为shuffle
4. 所有spark算子中, 没有shuffle操作的算子速度快
5. coalesce 就是合并分区操作, 没有shuffle操作
6. coalesce 和 repartition的区别
    1. coalesce重新分区, 可以选择是否进行shuffle过程
    2. repartition实际上调用的是coalesce, 默认时进行shuffle的