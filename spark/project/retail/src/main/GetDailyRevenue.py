from pyspark import SparkConf, SparkContext

conf = SparkContext.setAppName("Daily Revenue Per Product").setMaster("yarn-client")
sc = SparkContext.conf(conf=conf)

orders = sc.textFile("retail_db/orders")
orderItems = sc.textFile("retail_db/order_items")

for i in orders.take(1):print i

for i in orderItems.take(1):print i    
    
#print orders.map(lambda o:o.split(",")[3]).distinct().collect()    

ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE","CLOSED"] )

ordersMap = ordersFiltered.map(lambda o: ( int(o.split(",")[0]) , o.split(",")[1]) )
orderItemsMap = orderItems.map(lambda oi: ( int(oi.split(",")[1]), ( int(oi.split(",")[2]) ,float(oi.split(",")[4]) ) ) )
                               
ordersOrderItemsJoin = ordersMap.join(orderItemsMap)
#print ordersOrderItemsJoin.take(1)
#[(65536, (u'2014-05-16 00:00:00.0', (957, 299.98)))]

orderJoinMap = ordersOrderItemsJoin.map(lambda k: ( (k[1][0],k[1][1][0]) , k[1][1][1] ) )
#print orderJoinMap.take(1)
#[((u'2014-05-16 00:00:00.0', 957), 299.98)]

revenuePerProduct = orderJoinMap.reduceByKey(lambda x,y:x+y)
#print revenuePerProduct.take(5)

orderProductMap = revenuePerProduct.map(lambda r: (r[0][1], r))

products = open("/home/zubairidrees/hadoop-2.7.3/data/retail_db/products/part-00000").read().splitlines()
productRDD = sc.parallelize(products)
productMap = productRDD.map(lambda p: (int(p.split(",")[0]),p.split(",")[2]  ))

ordersProductJoin = orderProductMap.join(productMap)

#print ordersProductJoin.take(1)
#[ (24,  ( ( (u'2013-12-04 00:00:00.0', 24), 159.98), 'Elevation Training Mask 2.0'))]
productRevenueByDate = ordersProductJoin.map(lambda op: ((op[1][0][0][0], -op[1][0][1]), \
                                                         ( op[1][0][0][0]+ ","+str( op[1][0][1]) +","+ op[1][1] ) ) )

#print productRevenueByDate.take(1)

print "Sorted Product"
sortedProducts = productRevenueByDate.sortByKey().map(lambda s: s[1])
#for i in sortedProducts.take(2): print i
 
sortedProducts.saveAsTextFile("/user/zubairdrees/daily_revenue_by_product")