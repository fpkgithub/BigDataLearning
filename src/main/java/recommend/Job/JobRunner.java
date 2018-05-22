package recommend.Job;

import recommend.step1.MR1;
import recommend.step2.MR2;
import recommend.step3.MR3;
import recommend.step4.MR4;
import recommend.step5.MR5;

public class JobRunner
{
    public static void main(String[] args)
    {
        int status1 = -1;
        int status2 = -1;
        int status3 = -1;
        int status4 = -1;
        int status5 = -1;

        status1 = new MR1().run();
        if (status1 == 1)
        {
            System.out.println("Step1运行成功，开始运行Step2...");
            status2 = new MR2().run();
        }
        else
        {
            System.out.println("Step1运行失败...");
        }

        if (status2 == 1)
        {
            System.out.println("Step2运行成功，开始运行Step3...");
            status3 = new MR3().run();
        }
        else
        {
            System.out.println("Step2运行失败...");
        }

        if (status3 == 1)
        {
            System.out.println("Step3运行成功，开始运行Step4...");
            status4 = new MR4().run();
        }
        else
        {
            System.out.println("Step3运行失败...");
        }

        if (status4 == 1)
        {
            System.out.println("Step4运行成功，开始运行Step5...");
            status5 = new MR5().run();
        }
        else
        {
            System.out.println("Step4运行失败...");
        }

        if (status5 == 1)
        {
            System.out.println("Step5运行成功，结束...");
        }
        else
        {
            System.out.println("Step5运行失败...");
        }
    }
}
