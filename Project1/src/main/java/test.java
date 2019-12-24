public
class test extends ManagerSuperClass implements Runnable {
    @Override
    public
    void run() {
        System.out.println("test");
        while (continueRunning.get()){

        }
        System.out.println("done");
    }
}
