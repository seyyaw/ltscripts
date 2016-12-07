package scripts;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;

public class DateFormaterTester {
	public static void main(String[] args) throws ParseException {
		String date1 = "2012";
		String date2 = "2012-02";
		String date3 = "2012-02-03";
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date startDate = null;

		try {
			startDate = formatter.parse(date1);
		} catch (Exception e) {
			try {
				formatter = new SimpleDateFormat("yyyy-MM");
				startDate = formatter.parse(date1);
			} catch (Exception e2) {
				try {
					formatter = new SimpleDateFormat("yyyy");
					startDate = formatter.parse(date1);
				} catch (Exception e3) {
					// do nothing
				}
			}
		}
	
		formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		Date after = formatter.parse("1900-01-01");
		Date futureDate = new Date();
				if(startDate.after(after) && startDate.before(futureDate)) {
					String newDateString = formatter.format(startDate);
					System.out.println(newDateString);
				}


		LocalDate localDate = LocalDate.parse(date3);

		System.out.println(localDate);
	}
}
