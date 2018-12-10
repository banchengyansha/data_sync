package cn.itcast.main;

import cn.itcast.pojo.Book;
import cn.itcast.util.BinlogValue;
import cn.itcast.util.CanalDataParser;
import cn.itcast.util.DateUtils;
import cn.itcast.util.InnerBinlogEntry;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

public class SyncDataBootStart {

    private static Logger logger = LoggerFactory.getLogger(SyncDataBootStart.class);

    public static void main(String[] args) throws Exception {

        String hostname = "192.168.142.155";
        Integer port = 11111;
        String destination = "example";

        //获取CanalServer 连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination, "", "");

        //连接CanalServer
        canalConnector.connect();

        //订阅Destination
        canalConnector.subscribe();

        //轮询拉取数据
        Integer batchSize = 5*1024;
        while (true){
            Message message = canalConnector.getWithoutAck(batchSize);

            long messageId = message.getId();
            int size = message.getEntries().size();

            if(messageId == -1 || size == 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{

                //进行数据同步
                //1. 解析Message对象
                List<InnerBinlogEntry> innerBinlogEntries = CanalDataParser.convertToInnerBinlogEntry(message);


                //2. 将解析后的数据信息 同步到Solr的索引库中.
                syncDataToSolr(innerBinlogEntries);


            }

            //提交确认
            canalConnector.ack(messageId);

        }

    }

    private static void syncDataToSolr(List<InnerBinlogEntry> innerBinlogEntries) throws Exception {

        //获取solr的连接
        SolrServer solrServer = new HttpSolrServer("http://192.168.142.143:8080/solr");

        //遍历数据集合 , 根据数据集合中的数据信息, 来决定执行增加, 修改 , 删除操作 .
        if(innerBinlogEntries != null){
            for (InnerBinlogEntry innerBinlogEntry : innerBinlogEntries) {

                CanalEntry.EventType eventType = innerBinlogEntry.getEventType();

                //如果是Insert, update , 则需要同步数据到 solr 索引库
                if(eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE){
                    List<Map<String, BinlogValue>> rows = innerBinlogEntry.getRows();
                    if(rows != null){
                        for (Map<String, BinlogValue> row : rows) {
                            BinlogValue id = row.get("id");
                            BinlogValue name = row.get("name");
                            BinlogValue author = row.get("author");
                            BinlogValue publishtime = row.get("publishtime");
                            BinlogValue price = row.get("price");
                            BinlogValue publishgroup = row.get("publishgroup");

                            Book book = new Book();
                            book.setId(Integer.parseInt(id.getValue()));
                            book.setName(name.getValue());
                            book.setAuthor(author.getValue());
                            book.setPrice(Double.parseDouble(price.getValue()));
                            book.setPublishgroup(publishgroup.getValue());
                            book.setPublishtime(DateUtils.parseDate(publishtime.getValue()));


                            //导入数据到solr索引库
                            solrServer.addBean(book);
                            solrServer.commit();
                        }
                    }

                }else if(eventType == CanalEntry.EventType.DELETE){
                    //如果是Delete操作, 则需要删除solr索引库中的数据 .
                    List<Map<String, BinlogValue>> rows = innerBinlogEntry.getRows();
                    if(rows != null){
                        for (Map<String, BinlogValue> row : rows) {
                            BinlogValue id = row.get("id");

                            //根据ID删除solr的索引库
                            solrServer.deleteById(id.getValue());
                            solrServer.commit();

                        }
                    }

                }
            }
        }


    }

}
