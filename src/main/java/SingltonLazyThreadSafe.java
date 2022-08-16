public class SingltonLazyThreadSafe {

    static {
        System.out.println("Loading Outer");
    }

    private SingltonLazyThreadSafe() {
    }

    private static class SingltonLazyThreadSafeInner {
        static {
            System.out.println("Loading Inner");
        }
        private static SingltonLazyThreadSafe INSTANCE = new SingltonLazyThreadSafe();
    }

    public static SingltonLazyThreadSafe getInstance() {
        return SingltonLazyThreadSafeInner.INSTANCE;
    }

    public static void test() {
        System.out.println("Invoking Outer");
    }
}
