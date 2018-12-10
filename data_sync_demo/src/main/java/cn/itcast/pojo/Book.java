package cn.itcast.pojo;

import org.apache.solr.client.solrj.beans.Field;

import java.util.Date;

public class Book {

    @Field("id")
    private Integer id;

    @Field("book_name")
    private String name;

    @Field("book_author")
    private String author;

    @Field("book_publishtime")
    private Date publishtime;

    @Field("book_price")
    private Double price;

    @Field("book_publishgroup")
    private String publishgroup;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Date getPublishtime() {
        return publishtime;
    }

    public void setPublishtime(Date publishtime) {
        this.publishtime = publishtime;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getPublishgroup() {
        return publishgroup;
    }

    public void setPublishgroup(String publishgroup) {
        this.publishgroup = publishgroup;
    }

    @Override
    public String toString() {
        return "Book{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", author='" + author + '\'' +
                ", publishtime=" + publishtime +
                ", price=" + price +
                ", publishgroup='" + publishgroup + '\'' +
                '}';
    }
}
