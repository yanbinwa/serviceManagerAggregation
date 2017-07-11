package yanbinwa.iAggregation.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.iAggregation.service.AggregationService;


@RestController
@RequestMapping("/aggregation")
public class AggregationController
{
    @Autowired
    AggregationService aggregationService;
    
    @RequestMapping(value="/getServiceName",method=RequestMethod.GET)
    public String getServiceName() throws ServiceUnavailableException
    {
        return aggregationService.getServiceName();
    }
    
    @RequestMapping(value="/isServiceReady",method=RequestMethod.GET)
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        return aggregationService.isServiceReady();
    }
    
    @RequestMapping(value="/getServiceDependence",method=RequestMethod.GET)
    public String getServiceDependence() throws ServiceUnavailableException
    {
        return aggregationService.getServiceDependence();
    }
    
    @RequestMapping(value="/startManageService",method=RequestMethod.POST)
    public void startManageService()
    {
        aggregationService.startManageService();
    }
    
    @RequestMapping(value="/stopManageService",method=RequestMethod.POST)
    public void stopManageService()
    {
        aggregationService.stopManageService();
    }
    
    @ResponseStatus(value=HttpStatus.NOT_FOUND, reason="webService is stop")
    @ExceptionHandler(ServiceUnavailableException.class)
    public void serviceUnavailableExceptionHandler() 
    {
        
    }
    
}
