package izolotov.secondarysort;

import org.junit.Test;

public class AppTest {

    @Test
    public void test() {
        App.main(new String[] {
                "/media/izolotov/f163581d-53e6-4529-80d8-b822a479c7ab/dev/secondarysort/src/test/resources/urls.txt",
                "/tmp/secondarysort",
                "/tmp/result",
                "/media/izolotov/f163581d-53e6-4529-80d8-b822a479c7ab/dev/secondarysort/src/test/resources/blacklist.txt"});
    }

}
