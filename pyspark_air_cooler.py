import pyspark
sc=pyspark.SparkContext('local[*]')
sc.setLogLevel('WARN')


#Loading the file into an RDD and splitting the records based on the delimiter 

#Load the building file and perform the cleansing
buildingRDD = sc.textFile("/home/hadoop/Learnbay/pyspark/air_cooler/building.csv")
build_hdr=buildingRDD.first()
build_fnl=buildingRDD.filter(lambda x:x!=build_hdr)

#Load the hvac file and perform the cleansing
hvacRDD = sc.textFile("/home/hadoop/Learnbay/pyspark/air_cooler/HVAC.csv")
hvac_hdr=hvacRDD.first()
hvac_fnl=hvacRDD.filter(lambda x:x!=hvac_hdr)

#perform the join between the two RDD's

build_fnl.persist()
hvac_fnl.persist()

build_fnl1 = build_fnl.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(x[0],x[4]))

hvac_fnl1 = hvac_fnl.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(x[6],int(x[2]))).filter(lambda x:x[1]>5)

join = build_fnl1.join(hvac_fnl1)

grp = join.groupBy(lambda x:x[1][0])

res = grp.map(lambda x:(x[0],len(list(x[1])))).sortBy(lambda x:x[1])

res.coalesce(1).saveAsTextFile("/home/hadoop/Learnbay/pyspark/air_cooler/op1")

hvac_fnl2 = hvac_fnl.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(x[6],int(x[2]))).filter(lambda x:x[1]<5)

join_2 = build_fnl1.join(hvac_fnl2)

grp_2 = join_2.groupBy(lambda x:x[1][0])

res_2 = grp_2.map(lambda x:(x[0],len(list(x[1])))).sortBy(lambda x:x[1])

res_2.coalesce(1).saveAsTextFile("/home/hadoop/Learnbay/pyspark/air_cooler/op2")

