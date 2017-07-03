import java.util.*;
import java.util.concurrent.*;

class PrimaryBackupServer implements Runnable {
    List<Command> acceptedCommands = new ArrayList<>();
    boolean primary;
    BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    MessageReceiver messageReceiver;

    int port;
    int peerPorts[];

    boolean stopThread;

    PrimaryBackupServer(int port, int [] peerPorts) {
        this.port = port;
        this.peerPorts = peerPorts;

        messageReceiver = new MessageReceiver(messageQueue, port);
        new Thread(this).start();
    }

    boolean isPrimary() {
        return primary;
    }

    void makePrimary() {
        primary = true;
    }

    void makeBackup() {
        primary = false;
    }

    void process() {
        try {
            while (!stopThread) {
                Command c = (Command)messageQueue.take();

                if (c.getType().equals("START_COMMAND")) {
                    StartCommand sc = (StartCommand)c;
                    if (!primary) {
                        // forward to primary.
                        continue;
                    }

                    BackupCommand bc = new BackupCommand();
                    sc.copy(bc);

                    acceptedCommands.add(sc);

                } else if (c.getType().equals("BACKUP_COMMAND")) {
                    if (primary) {
                        // ignore or do something?
                        continue;
                    }
                } else if (c.getType().equals("HEARTBEAT_COMMAND")) {
                } else {
                    System.out.println("Unrecognized command " + c);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        process();
    }
}
