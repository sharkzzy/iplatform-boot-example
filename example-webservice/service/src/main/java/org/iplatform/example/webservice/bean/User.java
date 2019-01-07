package org.iplatform.example.webservice.bean;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author liangruijia
 */
@Getter
@Setter
@ToString
public class User {
    private String userId;
    private String userName;
    private int age;
}
