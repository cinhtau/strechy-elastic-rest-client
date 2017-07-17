package net.cinhtau.data;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Person {

    // @formatter:off
    @Getter @Setter private String id;
    @Getter @Setter private int index;
    @Getter @Setter private String guid;
    @Getter @Setter private boolean active;
    @Getter @Setter private String balance;
    @Getter @Setter private String picture;
    @Getter @Setter private int age;
    @Getter @Setter private String eyeColor;
    @Getter @Setter private String name;
    @Getter @Setter private String gender;
    @Getter @Setter private String company;
    @Getter @Setter private String email;
    @Getter @Setter private String phone;
    @Getter @Setter private String address;
    @Getter @Setter private String about;
    @Getter @Setter private String registered;
    @Getter @Setter private float latitude;
    @Getter @Setter private float longitude;
    @Getter @Setter private String greeting;
    @Getter @Setter private String favoriteFruit;
    // @formatter:on

}
