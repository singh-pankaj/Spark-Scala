String sDate1="01/03/2018";  
		SimpleDateFormat formatter1=new SimpleDateFormat("dd/MM/yyyy");  
		 Date Startdate=new SimpleDateFormat("dd/MM/yyyy").parse(sDate1);  
		    System.out.println(sDate1+"\t"+Startdate);  
		
		    String sDate2="01/11/2017";
		    Date endDate=new SimpleDateFormat("dd/MM/yyyy").parse(sDate2);  
		    System.out.println(sDate2+"\t"+endDate);  
		
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.setTime(Startdate);
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.setTime(endDate);

		int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
		int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);
		
		System.out.println("diffrence is "+diffMonth);

 def getPatientgroupByandKeyOrderBy(rdd: RDD[CassandraRow]): Array[(String,Iterable[Long])] = {

    val lockingrdd = rdd.filter(l => !l.isNullAt("patientuid") && !l.isNullAt("encounterdate") && l.getString("patientuid").equals("45850458-5bf9-4b70-b721-8b6b5358721f"))
      .map(l => (l.getString("patientuid"), l.getDate("encounterdate").getTime)).groupByKey().collect() //.map(r => (r._1,r._2)).collect()
      //.sortBy(_._2).collect()
    // .map( x => (x._1,new DateTime(x._2)))    //collect().sortBy( x => x._2.max)
     //.reduceByKey((x, y) => if (x.isBefore(y)) x else y).collect()


   for(x<-lockingrdd){
     println("=======patientid===========")
     println(x)
     var mindate = new DateTime(x._2.min)
     println("max is "+new DateTime(x._2.min))



     var c:Iterable[Long]=null


     var sortedList = new util.ArrayList[Long]()
     for(z <-x._2){

       sortedList.add(z)
     /*  if(new DateTime(z).isAfter(mindate) && mindate.plusDays(30).isAfter(new DateTime(z))) {

           c=z
       }
      c.min
     }*/

   }

    val xcv= Collections.sort()
   // rdd.filter( r => lockingrdd.contains(r.getString("patientuid"),r.getDate("encounterdate"))).collect().toList
  //  rdd.foreach( r => )
    lockingrdd

  }




  def getPatientgroupByandKeyOrderBy(rdd: RDD[CassandraRow]): Array[(String,Array[Long])] = {

    val lockingrdd = rdd.filter(l => !l.isNullAt("patientuid") && !l.isNullAt("encounterdate") && l.getString("patientuid").equals("45850458-5bf9-4b70-b721-8b6b5358721f"))
      .map(l => (l.getString("patientuid"), l.getDate("encounterdate").getTime)).groupByKey()
      .map(r => (r._1,r._2.toArray.sorted)).collect()

    //.collect() //.map(r => (r._1,r._2)).collect()
     // .sortBy(_._2,false).collect()
    // .map( x => (x._1,new DateTime(x._2)))    //collect().sortBy( x => x._2.max)
     //.reduceByKey((x, y) => if (x.isBefore(y)) x else y).collect()




   for(x<-lockingrdd){
     
     var min = new DateTime(x._2(0))
     var req = ""+min

     x._2.filter( dt => min.plusDays(30).isBefore(new DateTime(dt)))

     for(dt <-x._2) {

       if ( min.plusDays(29).isBefore(new DateTime(dt))){

         min = new DateTime(dt)
         req = req+" "+min
       }


     }

     println(" required date ===>>"+ req)

   }

    lockingrdd

  }

