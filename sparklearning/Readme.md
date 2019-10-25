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
7. 其他算子
    1. groupBy
    2. union
    3. subtract
    4. intersection
    5. cartesian
    6. zip: 分区数要相等, 每个分区里的元素个数要相等
8. join 和 cogroup的区别
9. 两类算子
    1. 转换算子
    2. 行动算子
        1. reduce
        2. collect
        3. count
        4. first
        5. take
        6. takeOrdered
        7. aggregate: 分区内和分区间都加上了初始值
        8. fold
        9. saveAsTextFile
        10. saveAsSequenceFile
        11. saveAsObjectFile
        12. countByKey
        13. forEach