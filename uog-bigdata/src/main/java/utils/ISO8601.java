package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Helper class for handling ISO 8601 strings of the following format:
 * "2008-03-01T13:00:00+01:00". It also supports parsing the "Z" timezone.
 */
public final class ISO8601 {
	/** Transform timestamp(ms) to Calendar */
	public static String timestampToString(final long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		Date date = calendar.getTime();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		String formatted = sdf.format(date);

		return formatted;
	}

	/** Transform ISO 8601 string to timestamp. */
	public static long toTimeMS(final String iso8601string)
			throws ParseException {
		String s = iso8601string.replace("Z", "+00:00");
<<<<<<< HEAD
=======

>>>>>>> 782605c42ba416a5790ff3f9d3c89191f47949d7
		try {
			s = s.substring(0, 22) + s.substring(23); // to get rid of the ":"
		} catch (IndexOutOfBoundsException e) {
			throw new ParseException("Invalid length", 0);
		}
		Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s);

		return date.getTime();
	}
}
<<<<<<< HEAD
=======

//		if(iso8601string.substring(20) ==  "+0:00") // To handle special cases:"2007-09-21T17:46:13+0:00"
//			s = iso8601string.replace("Z", "+0:00");
//		else
//			s = iso8601string.replace("Z", "+00:00");
>>>>>>> 782605c42ba416a5790ff3f9d3c89191f47949d7
