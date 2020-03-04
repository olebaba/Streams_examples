package Classes;

import org.apache.kafka.streams.examples.pageview.PageViewTypedDemo;

import java.util.Locale;

public class RankingElements implements PageViewTypedDemo.JSONSerdeCompatible {
    public Integer userId;
    public Integer rankingElementId;
    public String posistion;


}
