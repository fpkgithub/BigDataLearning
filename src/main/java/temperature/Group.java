package temperature;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator
{
    public Group()
    {
        super(MyKeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
        MyKeyPair k1 = (MyKeyPair) a;
        MyKeyPair k2 = (MyKeyPair) b;

        return Integer.compare(k1.getYear(), k2.getTemperature());

    }
}
