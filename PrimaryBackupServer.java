import java.util.*;
import java.util.concurrent.*;

class PrimaryBackupServer implements Runnable {
    List<Command> acceptedCommands = new ArrayList<>();
    BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    Queue<Command> blockedCommandQueue = new LinkedList<>();
    Map<String, Command> blockedAckMap = new HashMap<>();
    Map<String, Command> blockedConfigChangeAckMap = new HashMap<>();
    List<Command> stoppedCommands = new ArrayList<>();
    MessageReceiver messageReceiver;
    Map<Integer, Long> healthyTime = new HashMap<>();

    int port;
    List<Integer> portChain;

    boolean stopThread;
    boolean stopped;

    PrimaryBackupServer(int port, List<Integer> portChain) {
        this.port = port;
        setServerChain(portChain);

        messageReceiver = new MessageReceiver(messageQueue, port);
        new Thread(messageReceiver).start();
    }

    boolean isUserCommand(Command c) {
        return c.getType().equals("START_COMMAND");
    }

    void addCommand() {
        StartCommand sc = new StartCommand();

        sc.setId(UUID.randomUUID().toString());
        sc.setTimeStamp(getCurrentTimeStamp());
        sc.setData("Data");
        try {
            sendToHost(sc, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    boolean isBlockedForAck() {
        return blockedAckMap.size() > 0;
    }

    boolean isBlockedForConfigChangeAck() {
        return blockedConfigChangeAckMap.size() > 0;
    }

    void setServerChain(List<Integer> portChain) {
        this.portChain = portChain;
    }

    // needs synchronization?
    void setPortChain(List<Integer> portChain) {
        this.portChain = portChain;
    }

    boolean isStopped() {
        return stopped;
    }

    void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    void sendAckToUser(Command c) {
        System.out.println("Send ack to user for " + c);
    }

    void sendToHost(Message m, int port) {
        m.setSenderPort(this.port);
        MessageSender.send(m, port);
    }

    void sendToPrimary(Message m) {
        m.setSenderPort(this.port);
        MessageSender.send(m, portChain.get(0));
    }

    void sendToFirstBackup(Message m) {
        m.setSenderPort(this.port);
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
            m.setSenderPort(this.port);
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

    void processAsPrimary(Command c) {
        if (c.getType().equals("START_COMMAND")) {
            // update its copy.
            CooperateCommand cc = new CooperateCommand();

            c.copy(cc);
            sendToFirstBackup(cc);
            blockedAckMap.put(c.getId(), c);
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
        } else if (c.getType().equals("CONFIG_CHANGE_COMMAND")) {
            // send to all other hosts, maybe only the ones in the new config.
            // accept the config change.
            // send command to others to resume new user traffic after processing stopped traffic.
        } else if (c.getType().equals("ACK_COMMAND")) {
            blockedAckMap.remove(c.getId());
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
        } else if (c.getType().equals("START_COMMAND")) {
            ForwardCommand fc = new ForwardCommand();

            c.copy(fc); 
            sendToPrimary(fc);
            blockedAckMap.put(c.getId(), c);
        } else if (c.getType().equals("REPL_COMMAND")) {
            sendToNextBackup(c);
        } else if (c.getType().equals("CONFIG_CHANGE_COMMAND")) {
            setStopped(true);
            // set new config.
        } else if (c.getType().equals("ACK_COMMAND")) {
            blockedAckMap.remove(c.getId());
        }
    }

    void processCommand(Command c) {
        if (isPrimary()) {
            processAsPrimary(c);
            if (isStopped() && !isBlockedForAck() && isBlockedForConfigChangeAck()) {
                // all acks received, send command to resume user commands for all hosts
            }
        } else {
            processAsBackup(c);
            if (isStopped() && !isBlockedForAck()) {
                // all acks received apply config change and return ack
            }
        }
    }

    void process() {
        try {
            while (!stopThread) {
                Command c;

                c = (Command)messageQueue.take();

                if (isStopped()) {
                    if (isUserCommand(c)) {
                        stoppedCommands.add(c);
                        continue;
                    } else if (c.getType().equals("RESUME_COMMAND")) {
                        setStopped(false);
                        // process all stopped start commands before any new ones.
                        for (Command sc : stoppedCommands) {
                            processCommand(sc);
                        }
                        continue;
                    }
                }
                processCommand(c);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        process();
    }
}
