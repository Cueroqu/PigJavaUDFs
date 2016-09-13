package myudfs;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

/**
 * Created by zhun.qu on 9/13/16.
 */
public class StringToDate extends EvalFunc<DateTime> {
    public DateTime exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            String str = (String)input.get(0);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dt = formatter.parseDateTime(str);
            return dt;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
