import java.util.*;

class ConfigChangeCommand extends Command {
    List<Integer> portChain;

    ConfigChangeCommand() {
        setType("CONFIG_CHANGE_COMMAND");
    }

    List<Integer> getPortChain() {
        return portChain;
    }

    void setPortChain(List<Integer> portChain) {
        this.portChain = portChain;
    }
}
