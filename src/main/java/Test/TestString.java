package Test;

public class TestString
{
    public static void main(String[] args)
    {
        String str = "asfh/hdkjsa/123";
        System.out.println(str.substring(str.lastIndexOf("/")+1, str.length()));
    }
}
