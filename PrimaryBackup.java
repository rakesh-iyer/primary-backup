import java.util.*;

class PrimaryBackup {
    static boolean stopped;

    public static void main(String args[]) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("Not enough hosts");
            return;
        }

        List<Integer> portChain = new ArrayList<>();
        for (int i = 0; i < args.length - 1; i++) {
            portChain.add(Integer.valueOf(args[i]));
        }

        // zero-start for idx
        int idx = Integer.valueOf(args[args.length-1]);
        if (idx > args.length - 2) {
            System.out.println("Idx is wrong");
            return;
        }

        PrimaryBackupServer pb = new PrimaryBackupServer(Integer.valueOf(args[idx]), portChain);
        Thread t = new Thread(pb);

        t.start();

        while (!stopped) {
            Thread.sleep(60000);
            pb.addCommand();
        }

        t.join();
    }
}
