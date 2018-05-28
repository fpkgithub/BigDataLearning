package temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<MyKeyPair, Text>
{

    @Override
    public int getPartition(MyKeyPair myKeyPair, Text text, int i)
    {
        return (myKeyPair.getYear() * 127) % i;
    }
}
