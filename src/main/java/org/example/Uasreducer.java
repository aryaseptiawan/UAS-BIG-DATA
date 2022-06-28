package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Uasreducer extends Reducer<Text, Text, NullWritable, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        String[] dataSupermart = key.toString().split(",");
        String OrderID = dataSupermart[0];
        String CustomerName = dataSupermart[1];
        String Category = dataSupermart[2];
        String City = dataSupermart[3];

        int Discount = 0;
        int Sales = 0;
        int Profit = 0;

        for(Text value : values)
        {
            String[] Retail = value.toString().split("#");

            Discount += Integer.parseInt(Retail[0]);
            Sales += Integer.parseInt(Retail[1]);
            Profit += Integer.parseInt(Retail[2]);
        }

        context.write(NullWritable.get(), new Text(OrderID + "," + CustomerName + "," + "," + Category + "," + City + "," + String.valueOf(Discount) + "," + String.valueOf(Sales) + "," + String.valueOf(Profit)));
    }
}
