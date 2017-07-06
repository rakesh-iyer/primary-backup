import java.util.*;
import java.util.concurrent.*;

class PrimaryBackupServer implements Runnable {
    List<Command> acceptedCommands = new ArrayList<>();
    boolean primary;
    BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    Queue<Command> blockedCommandQueue = new LinkedList<>();
    Map<String, Command> blockedAckMap = new HashMap<>();
    MessageReceiver messageReceiver;
    Map<Integer, Long> healthyTime = new HashMap<>();

    int port;
    List<Integer> portChain;

    boolean stopThread;

    PrimaryBackupServer(int port, List<Integer> portChain) {
        this.port = port;
        setServerChain(portChain);

        messageReceiver = new MessageReceiver(messageQueue, port);
        new Thread(this).start();
    }

    void setServerChain(List<Integer> portChain) {
        this.portChain = portChain;
    }

    // needs synchronization?
    void setPortChain(List<Integer> portChain) {
        this.portChain = portChain;
    }

    void sendAckToUser(Command c) {
        System.out.println("Send ack to user for " + c);
    }

    void sendToHost(Message m, int port) {
        MessageSender.send(m, port);
    }

    void sendToPrimary(Message m) {
        MessageSender.send(m, portChain.get(0));
    }

    void sendToFirstBackup(Message m) {
        MessageSender.send(m, portChain.get(1));
    }

    void sendToNextBackup(Message m) {
        int i;

        for (i = 0; i < portChain.size(); i++) {
            if (portChain.get(i) == port) {
                break;
            }
        }

        // only send if there is a backup.
        if (i + 1 < portChain.size()) {
            MessageSender.send(m, portChain.get(i+1));
        }
    }

    void replyToClient(Command c) {
        return;
    }

    boolean isPrimary() {
        return port == portChain.get(0);
    }

    boolean isFirstBackup() {
        return port == portChain.get(1);
    }

    long getCurrentTimeStamp() {
        return System.currentTimeMillis();
    }

    boolean isBlockedForAck() {
        return blockedAckMap.size() > 0;
    }

    void processAsPrimary(Command c) {
        if (c.getType().equals("START_COMMAND")) {
            // update its copy.
            CooperateCommand cc = new CooperateCommand();

            c.copy(cc);
            sendToFirstBackup(cc);
            blockedAckMap.put(c.getId(), c);
        } else if (c.getType().equals("ACK_COMMAND")) {
            Command cAcked  = blockedAckMap.remove(c.getId());

            System.out.println("Got ack for " + cAcked);
        } else if (c.getType().equals("FORWARD_COMMAND")) {
            // update its copy.
            int forwardingPort = c.getSenderPort();

            AckCommand ac = new AckCommand();
            ac.setId(c.getId());

            ReplCommand rc = new ReplCommand();
            c.copy(rc);

            sendToFirstBackup(rc);
            sendAckToUser(c);
            sendToHost(ac, forwardingPort);
        }
    }

    void processAsBackup(Command c) { 
        if (c.getType().equals("COOPERATE_COMMAND")) {
            if (!isFirstBackup()) {
                System.out.println("this is an error, handle later.");
                return;
            }

            AckCommand ac = new AckCommand();
            ac.setId(c.getId());

            ReplCommand rc  = new ReplCommand();
            c.copy(rc);

            // no-op if this is the last backup in chain.
            sendToNextBackup(rc);

            sendAckToUser(c);
            sendToPrimary(ac);
        } else if (c.getType().equals("REPL_COMMAND")) {
            sendToNextBackup(c); 
        } else if (c.getType().equals("START_COMMAND")) {
            ForwardCommand fc = new ForwardCommand();

            c.copy(fc); 
            sendToPrimary(fc);
            blockedAckMap.put(c.getId(), c);
        }
    }

    void process() {
        try {
            while (!stopThread) {
                Command c;
                // Initial impl - dont worry about ack timeouts or timestamps for now.
                // if you are blocked for ack then process message queue for any acks first.
                if (isBlockedForAck() || blockedCommandQueue.size() == 0) {
                    c = (Command)messageQueue.take();
                } else {
                    c = blockedCommandQueue.remove();
                }

                // if blocked for ack just queue any other commands.
                if (isBlockedForAck() && !c.getType().equals("ACK_COMMAND")) {
                    blockedCommandQueue.add(c);
                    continue;
                }

                if (isPrimary()) {
                    processAsPrimary(c);
                } else {
                    processAsBackup(c);
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
