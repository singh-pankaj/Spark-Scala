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

