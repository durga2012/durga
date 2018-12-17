/*   ------------------------------------------------------------------------------------------------------------------------------   
 * Product/Project Name:	          Al Hail Orix
 * Module:	         		  Insertion
 * File:                                  MetaData.java
 * Purpose:  This File handles Inserting the metadata.
 ------------------------------------------------------------------------------------------------------------------------------------   */
package com.newgen.MetaData;

import com.amazonaws.util.json.JSONObject;
import com.newgen.Utils.CommonFunction;
import com.newgen.Utils.CommonLogger;
import com.newgen.MetaData.MetaDataDocumentDetails;
import com.newgen.wfdesktop.xmlapi.WFXmlResponse;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author durga.a
 */
@Path("/Insertion")
public class MetaData {

    @POST
    @Path("/MetaData")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MetaDataResponse Serv(MetaDataRequest req) throws IOException {
        String Status = null, Description = null;
        MetaDataResponse ObjRes = new MetaDataResponse();
        CommonLogger ObjCLOG = new CommonLogger();
        CommonFunction ObjCF = new CommonFunction();
        ObjCLOG.loadlogConfig();
        boolean Insert = true, boolREADINI;
        String CabinetName = "";
        String Query = "";
        String SessionID = "", result = "";
        WFXmlResponse xmlProcessList = null;
        boolREADINI = ObjCF.ReadINIProperties(ObjCLOG);
        if (boolREADINI) {
            try {
                ObjCLOG.PROCLOGGER().info("System Field Data:");
                ObjCLOG.PROCLOGGER().info("Inserting the data in Master Table");
                for (int i = 0; i < req.getDocumentDetails().size(); i++) {
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getDMSDOCID());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getDMSStatusCode());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getDMSStatusDescription());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getDocUploadDateTime());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getEntryDateTime());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getLMSStatusCode());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getLMSStatusDescription());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getProcessDateTime());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getProcessFlag());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getS3BucketName());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getS3DocumentName());
                    System.out.println("arraylist getDMSDOCID" + req.getDocumentDetails().get(i).getS3FolderPath());
                }
                System.out.println("ReferenceNumber" + req.getReferenceNumber());
                System.out.println("LoanRequestID" + req.getLOANRequestID());
                System.out.println("TransactionNumber" + req.getTransactionNumber());
                if (req.getDocumentDetails().size() > 0) {
                    ObjCLOG.PROCLOGGER().info("haii");
                    SessionID = ObjCF.getNGDBConnection1(ObjCLOG);
                    for (int j = 0; j < req.getDocumentDetails().size(); j++) {
                        System.out.println("Inside loop");
                        Query = "Insert into  dbo.[NG_Master_LMSS3DOCS ](LOANRequest_ID,Transaction_Number,ReferenceNumber,DocUpload_DateTime,Entry_DateTime,S3_BucketName,S3_FolderPath,S3_DocumentName,LMS_StatusCode,LMS_Status_Description,DMS_StatusCode,DMS_Status_Description,Process_DateTime,Process_Flag,DMSDOC_ID) values "
                                + "('" + req.getLOANRequestID() + "','" + req.getTransactionNumber() + "','" + req.getReferenceNumber() + "',convert(date,'" + req.getDocumentDetails().get(j).getDocUploadDateTime() + "'),convert(date,'" + req.getDocumentDetails().get(j).getEntryDateTime() + "'),'" + req.getDocumentDetails().get(j).getS3BucketName() + "','" + req.getDocumentDetails().get(j).getS3FolderPath() + "','" + req.getDocumentDetails().get(j).getS3DocumentName() + "','" + req.getDocumentDetails().get(j).getLMSStatusCode() + "','" + req.getDocumentDetails().get(j).getLMSStatusDescription() + "','" + req.getDocumentDetails().get(j).getDMSStatusCode() + "','" + req.getDocumentDetails().get(j).getDMSStatusDescription() + "',convert(date,'" + req.getDocumentDetails().get(j).getProcessDateTime() + "'), '" + req.getDocumentDetails().get(j).getProcessFlag() + "' ,'" + req.getDocumentDetails().get(j).getDMSDOCID() + "' )";
                        System.out.println("sample" + j + Query);
                        String Outputxml = ObjCF.ExecuteCommonQuery(Query, SessionID, ObjCLOG);
                        ObjCLOG.PROCLOGGER().info("OutputXML for insert query" + Outputxml);
                        xmlProcessList = new WFXmlResponse(Outputxml);
                        result = xmlProcessList.getVal("MainCode");
                        ObjCLOG.PROCLOGGER().info("result" + result);
                        if (xmlProcessList.getVal("MainCode").equalsIgnoreCase("0")) {
                            ObjCLOG.PROCLOGGER().info("MetaData Inserted Sucessfully");
                            Status = "19";
                            Description = "MetaData Inserted Sucessfully";
                        } else {
                            ObjCLOG.PROCLOGGER().info("Failure in Insertion of MetaData");
                            Status = "21";
                            Description = "Looks like there is a problem in Inserting data";
                        }
                        ObjCLOG.PROCLOGGER().info("Status of Insertion" + result);
                        ObjRes.setStatus(Status);
                        ObjRes.setDescription(Description);
                    }
                } else {
                    Status = "15";
                    Description = "Oops!Request parameter is missing";
                }
            } catch (Exception e) {
                ObjCLOG.ERRLOGGER().info("Exception in Inserting Master Data" + e.getMessage());
            }
        }


        return ObjRes;
    }
}
