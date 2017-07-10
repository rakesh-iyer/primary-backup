class StartCommand extends Command {
    boolean forwarded;

    StartCommand() {
        super.setType("START_COMMAND");
    }

    boolean isForwarded() {
        return forwarded;
    }

    void setForwarded(boolean forwarded) {
        this.forwarded = forwarded;
    }
}
