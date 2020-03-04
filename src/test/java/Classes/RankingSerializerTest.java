package Classes;

import static org.junit.jupiter.api.Assertions.*;

class RankingSerializerTest {

    @org.junit.jupiter.api.Test
    void serdes() {
        RankingElements re = new RankingElements();
        re.rankingElementId = 1;
        re.posistion = "0";
        re.userId = 2;

        System.out.println(re);

        RankingSerializer rs = new RankingSerializer();
        byte[] reBytes = rs.serialize("test", re);

        System.out.println(reBytes);

        RankingDeserializer rd = new RankingDeserializer();

        System.out.println(rd.deserialize("test", reBytes));
    }
}