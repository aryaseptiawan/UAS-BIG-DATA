package org.example;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Uasmapper extends Mapper<Object, Text, Text, Text>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        try
        {
            if(value.toString().contains("Region"))
                return;
            else
            {
                String data = value.toString();
                String[] columns = data.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                String OrderID = columns[1];
                String CustomerName = columns[3];
                String Category = columns[2];
                String City = columns[7];

                String Discount = columns[8];
                String Sales = columns[9];
                String Profit = columns[10];

                context.write(new Text(OrderID + "," + CustomerName + "," + Category + "," + City), new Text(Discount + "#" + Sales + "#" + Profit));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
