
package driverclass;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text retailerCity = new Text();
    private IntWritable salesAmount = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 6) {
            String retailer = fields[0].trim();
            String city = fields[2].trim();
            String price = fields[4].trim().replace("$", "").replace(".", "");
            int unitsSold = Integer.parseInt(fields[5].trim());
            int totalSales = Integer.parseInt(price) * unitsSold;

            retailerCity.set(retailer + ", " + city);
            salesAmount.set(totalSales);
            context.write(retailerCity, salesAmount);
        }
    }
}


