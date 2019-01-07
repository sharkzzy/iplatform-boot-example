package org.iplatform.example.webservice.service;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

/**
 * @author liangruijia
 */
@WebService
public interface CityService {

    /**
     * 根据城市id获取城市名称
     * @param cityId 城市id
     * @return
     */
    @WebMethod
    String getCityName(@WebParam(name = "cityId") String cityId);
}
