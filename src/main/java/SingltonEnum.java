public enum SingltonEnum {
    INSTANCE;

    public synchronized SingltonEnum getInstance() {
        return INSTANCE;
    }
}
