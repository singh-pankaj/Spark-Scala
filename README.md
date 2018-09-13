
  def getDaysDiff(startDate: String,endDate :String): Long ={
    val s1 = SIMPLE_DATE_FORMAT.parse(startDate);
    val s2 = SIMPLE_DATE_FORMAT.parse(endDate)
    import java.util.concurrent.TimeUnit
    val diffInMillies = Math.abs(s1.getTime - s2.getTime)
    return TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS)

  }
