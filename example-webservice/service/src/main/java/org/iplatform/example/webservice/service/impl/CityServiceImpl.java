package org.iplatform.example.webservice.service.impl;

import org.iplatform.example.webservice.bean.City;
import org.iplatform.example.webservice.bean.User;
import org.iplatform.example.webservice.service.CityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.jws.WebService;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liangruijia
 */
@WebService(targetNamespace = "http://service.webservice.example.iplatform.org/", endpointInterface = "org.iplatform.example.webservice.service.CityService")
@Service
public class CityServiceImpl implements CityService {
    private Logger logger = LoggerFactory.getLogger(UserServiceImpl.class);
    private Map<String, City> cityMap = new HashMap<>();

    @PostConstruct
    public void init(){
        logger.info("插入城市数据");
        City city = new City();
        city.setId("1");
        city.setName("beijing");
        city.setPosition("beijing");
        cityMap.put(city.getId(), city);

        city = new City();
        city.setId("2");
        city.setName("shijiazhuang");
        city.setPosition("hebei");
        cityMap.put(city.getId(), city);

        city = new City();
        city.setId("2");
        city.setName("hangzhou");
        city.setPosition("zhejiang");
        cityMap.put(city.getId(), city);
    }
    @Override
    public String getCityName(String cityId) {
        String cityName = null;
        if (cityMap.containsKey(cityId)){
            cityName = cityMap.get(cityId).getName();
        }
        return cityName;
    }
}
