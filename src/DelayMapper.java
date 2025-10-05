import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("carrier")) {
            return;
        }

        String[] fields = value.toString().split(",", -1); // handle empty fields
        try {
            String carrier = fields[2];
            float arr_delay = Float.parseFloat(fields[15]);

            // Emit only if there's a valid delay (can also skip if negative)
            if (arr_delay > 0) {
                context.write(new Text(carrier), new FloatWritable(arr_delay));
            }
        } catch (Exception e) {
            // Handle parsing error or missing fields
        }
    }
}
