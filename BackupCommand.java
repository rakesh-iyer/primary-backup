class BackupCommand extends Command {
    boolean forwarded;

    BackupCommand() {
        super.setType("BACKUP_COMMAND");
    }

    boolean isForwarded() {
        return forwarded;
    }

    void setForwarded(boolean forwarded) {
        this.forwarded = forwarded;
    }
}
