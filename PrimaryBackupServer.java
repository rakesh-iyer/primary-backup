import java.util.*;
import java.util.concurrent.*;

class PrimaryBackupServer implements Runnable {
    List<Command> acceptedCommands = new ArrayList<>();
    boolean primary;
    BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    MessageReceiver messageReceiver;
    Map<Integer, Long> healthyTime = new HashMap<>();

    int port;
    int peerPorts[];
    int backupPort;
    int primaryPort;

    boolean stopThread;
    boolean blockedForAck;

    PrimaryBackupServer(int port, int [] peerPorts) {
        this.port = port;
        this.peerPorts = peerPorts;

        messageReceiver = new MessageReceiver(messageQueue, port);
        new Thread(this).start();
    }

    boolean isBlockedForAck() {
        return blockedForAck;
    }

    void setBlockedForAck(boolean blockedForAck) {
        this.blockedForAck = blockedForAck;
    }

    void sendToPrimary(Message m) {
        MessageSender.send(m, primaryPort);
    }

    void sendToBackup(Message m) {
        MessageSender.send(m, backupPort);
    }

    void replyToClient(Command c) {
        return;
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

    long getCurrentTimeStamp() {
        return System.currentTimeMillis();
    }

    void process() {
        try {
            while (!stopThread) {
                Command c = (Command)messageQueue.take();

                if (c.getType().equals("START_COMMAND")) {
                    StartCommand sc = (StartCommand)c;
                    if (!isPrimary()) {
                        // forward to primary.
                        sendToPrimary(sc);
                        continue;
                    }

                    if (isBlockedForAck()) {
                        continue;
                    }

                    BackupCommand bc = new BackupCommand();
                    sc.copy(bc);

                    acceptedCommands.add(sc);

                    if (sc.isForwarded()) {
                        replyToClient(sc);
                        bc.setForwarded(true);
                    } else {
                        setBlockedForAck(true);
                    }
                    sendToBackup(bc);
                } else if (c.getType().equals("BACKUP_COMMAND")) {
                    if (isPrimary()) {
                        // ignore or do something?
                        continue;
                    }

                    BackupCommand bc = new BackupCommand();
                    acceptedCommands.add(bc);
                    if (!bc.isForwarded()) {
                        replyToClient(bc);

                        AckCommand ack = new AckCommand();
                        // send ack to primary to unblock it.
                        sendToPrimary(ack);
                    } 
                } else if (c.getType().equals("ACK_COMMAND")) {
                    setBlockedForAck(false);
                } else if (c.getType().equals("HEARTBEAT_COMMAND")) {
                    healthyTime.put(c.getSenderPort(), getCurrentTimeStamp());
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
