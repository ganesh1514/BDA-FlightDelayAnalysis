import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportDelayMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("airport"))
            return;

        String[] fields = value.toString().split(",", -1);
        try {
            String airport = fields[4];
            float arr_delay = Float.parseFloat(fields[15]);
            if (arr_delay > 0) {
                context.write(new Text(airport), new FloatWritable(arr_delay));
            }
        } catch (Exception e) {
        }
    }
}
