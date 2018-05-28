package temperature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MapperJob extends Mapper<LongWritable, Text, MyKeyPair, Text>
{

    private Text outKey = new Text();
    private Text outValue = new Text();

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

        String line = value.toString();
        String[] values = line.split("\t");

        //System.out.println("value=" + value.toString());
        //System.out.println("values.length=" + values.length);


        if (values.length == 2)
        {
            try
            {
                //get year
                Date date = sdf.parse(values[0]);
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                int year = c.get(1);
                System.out.println("year=" + year);

                //get temperature
                String t = values[1].substring(0, values[1].indexOf("C"));
                System.out.println("t=" + t);

                //set values
                MyKeyPair k = new MyKeyPair();
                k.setYear(year);
                k.setTemperature(Integer.parseInt(t));

                //output key,value
                //Text t1 = new Text(t);
                //System.out.println("t1=" + t1);
                context.write(k, new Text(t));


            } catch (ParseException e)
            {
                e.printStackTrace();
            }
        }

    }
}
