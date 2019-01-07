package org.iplatform.example.webservice.config;

import org.apache.cxf.Bus;
import org.apache.cxf.bus.spring.SpringBus;
import org.apache.cxf.jaxws.EndpointImpl;
import org.apache.cxf.transport.servlet.CXFServlet;
import org.iplatform.example.webservice.service.CityService;
import org.iplatform.example.webservice.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.xml.ws.Endpoint;

/**
 * @author liangruijia
 */
@Configuration
public class WebServiceConfig {

    @Autowired
    private UserService userService;

    @Autowired
    private CityService cityService;

    @Bean
    public ServletRegistrationBean dispatcherServlet(){
        return new ServletRegistrationBean(new CXFServlet(), "/test/*");
    }

    @Bean(name = Bus.DEFAULT_BUS_ID)
    public SpringBus springBus(){
        return new SpringBus();
    }

    @Bean
    public Endpoint endpointUser() {
        Endpoint endpoint = new EndpointImpl(springBus(), userService);
        endpoint.publish("/user");
        return endpoint;
    }

    @Bean
    public Endpoint endpointCity() {
        Endpoint endpoint = new EndpointImpl(springBus(), cityService);
        endpoint.publish("/city");
        return endpoint;
    }
}
