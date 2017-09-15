package com.pfgc.javaBatch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class CIDB_ITEM_Converter {
	/*
	* Adding comments.....
	*/
		
	static Logger log = Logger.getLogger(LedyardItemXrefUtil4.class);
	String dbUrl ="jdbc:as400://RICWC01i/VBABU;naming=sql;extended dynamic=true;package=CIDBPKG;package add=true;package cache=true;package criteria=default;package library=Cdmdata;errors=full;libraries=,CIDB,SCDATA";
	String user="CIDB";
	String password = "CIDB01"; 
	
	String convertOpco = "680";
	String convertLib = "VBABU";
	String sourceLib = "CIDB";
	String itemXrefOrgTable = "CDAKITXF";
	String itemXrefConvTable = "CDAKCONV";
	String approvedItemTable = "CONV_ITM";
	
	String sql_syncCDAKCSAC =  " UPDATE CIDB.CSACPITMF SET ACITMNBR = (SELECT AKITMNBR FROM CIDB.CDAKITXF WHERE AKOPNBR = ACOPCO AND ACOPITM = AKOPITM ) "
							 + " WHERE ACOPITM  = ( SELECT AKOPITM FROM  CIDB.CDAKITXF  WHERE AKOPNBR = ACOPCO AND ACOPITM = AKOPITM ) "
							 + " AND ACOPCO = 680 AND ACITMNBR <> (SELECT AKITMNBR FROM CIDB.CDAKITXF WHERE AKOPNBR = ACOPCO AND ACOPITM = AKOPITM ) ";                         
	
	// CDAKITXF/CDAKCONV conversion sqls
	String sql_truncateCDAKCONV = "DELETE FROM VBABU.CDAKCONV ";
	// For production run use CIDB.CDAKITXF
	/*String sql_insertCDAKCONV =   " INSERT INTO VBABU.CDAKCONV ( AKOPNBR, AKOPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,  AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP )   "+                       
								  " ( SELECT AKOPNBR, AKITMNBR AS OPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,   AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP  " +
								  " FROM CIDB.CDAKITXF WHERE  AKOPNBR = 680 and AKITMNBR IN ( SELECT KEEP_PITM FROM VBABU.EITM"
								  + ") AND AKITMNBR <> 999999 ) ";*/
	
	//******** Copy the content from CIDB.CDAKITXF TO VBABU.CDAKORG before running this program
	//Table vbabu.CONV_ITM is from REFDATA.MIT_ITEM_XREF column TARGET_ERP_ITEM_NO; Query used SELECT [TARGET_ERP_ITEM_NO] as EITM FROM [MigrationService_Dev].[REFDATA].[PFG_ITEM_XREF] WHERE [VALID_ITEM_YN] = 'Y' AND TARGET_ERP_ITEM_NO IS NOT NULL;
	//Table VBABU.CDAKORG is the original copy from CIDB.CDAKITXF
	String sql_insertCDAKCONV =   " INSERT INTO VBABU.CDAKCONV ( AKOPNBR, AKOPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,  AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP )   "+                       
			  " ( SELECT AKOPNBR, AKITMNBR AS OPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,   AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP  " +
			  " FROM VBABU.CDAKORG WHERE  AKOPNBR = 680 and AKITMNBR IN ( SELECT EITM FROM VBABU.CONV_ITM "
			  + ") AND AKITMNBR <> 999999 ) ";
	
	String sql_deleteDupsCDAKCONV =  " DELETE  FROM VBABU.CDAKCONV WHERE ID IN (SELECT  MAX(ID) FROM VBABU.CDAKCONV GROUP BY AKITMNBR HAVING COUNT(AKITMNBR) >1  ) ";
	String sql_deleteCDAKITXF = 	 " DELETE  FROM CIDB.CDAKITXF WHERE AKOPNBR = 680 ";
	String sql_insertCDAKITXF =  	" INSERT INTO CIDB.CDAKITXF ( AKOPNBR, AKOPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,  AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP )   "+                       
								 	" ( SELECT AKOPNBR, AKOPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,   AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP  " +
								 	" FROM VBABU.CDAKCONV ) ";
	String sql_insertCDAKITXF999999 =  " INSERT INTO CIDB.CDAKITXF ( AKOPNBR, AKOPITM, AKITMNBR, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,  AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP )   "+                       
									   " ( SELECT AKOPNBR, 999999, 999999, AKPURGED, AKDWNLD, AKDWNST, AKDWNDT, AKREQUSER, AKREQNBR,   AKTRANTYP, AKUSERID, AKBYRNBR, AKSAPPROCP  " +
									   " FROM VBABU.CDAKCONV WHERE ID = ( SELECT MAX(ID) FROM VBABU.CDAKCONV )  ) ";
	
	
	// CSACPITMF/CSACCONV conversion sqls
		String sql_truncateCSACCONV = "DELETE FROM VBABU.CSACCONV ";
		String sql_insertCSACCONV =   " INSERT INTO VBABU.CSACCONV ( ACOPCO, ACOPITM, ACOPVND, ACOPVITM, ACBCC, ACPARITM, ACQMLT, ACBRDKWD, ACDESC1, ACDESC2, ACPACK, ACSIZE, ACARN, ACMFGITM, ACGWGT, ACNWGT, ACCWGTFL, ACRWGTFL, ACMPACK, ACTIHI, ACLEN, ACWIDTH, ACHGT, ACTMPZNE, ACGTIN, ACSHLFLF, ACSPP, ACHZMAT, ACCLS, ACPBH, ACARC, ACFSITM, ACITMNBR, ACSTAT, ACUSID, ACUSERID, ACTYPCD )   "+                       
									  " ( SELECT ACOPCO, ACITMNBR, ACOPVND, ACOPVITM, ACBCC, ACPARITM, ACQMLT, ACBRDKWD, ACDESC1, ACDESC2, ACPACK, ACSIZE, ACARN, ACMFGITM, ACGWGT, ACNWGT, ACCWGTFL, ACRWGTFL, ACMPACK, ACTIHI, ACLEN, ACWIDTH, ACHGT, ACTMPZNE, ACGTIN, ACSHLFLF, ACSPP, ACHZMAT, ACCLS, ACPBH, ACARC, ACFSITM, ACITMNBR, ACSTAT, ACUSID, ACUSERID, ACTYPCD   " +
									  " FROM VBABU.CSACORG WHERE  ACOPCO = 680 and ACITMNBR IN ( SELECT EITM FROM VBABU.CONV_ITM "
									  + ") AND ACITMNBR <> 999999 ) ";
		
		String sql_deleteDupsCSACCONV =  " DELETE  FROM VBABU.CSACCONV WHERE ID IN (SELECT  MAX(ID) FROM VBABU.CSACCONV GROUP BY ACITMNBR HAVING COUNT(ACITMNBR) >1  ) ";
		String sql_deleteCSACPITMF = 	 " DELETE  FROM CIDB.CSACPITMF WHERE ACOPCO = 680 ";
		String sql_insertCSACPITMF =  	" INSERT INTO CIDB.CSACPITMF ( ACOPCO, ACOPITM, ACOPVND, ACOPVITM, ACBCC, ACPARITM, ACQMLT, ACBRDKWD, ACDESC1, ACDESC2, ACPACK, ACSIZE, ACARN, ACMFGITM, ACGWGT, ACNWGT, ACCWGTFL, ACRWGTFL, ACMPACK, ACTIHI, ACLEN, ACWIDTH, ACHGT, ACTMPZNE, ACGTIN, ACSHLFLF, ACSPP, ACHZMAT, ACCLS, ACPBH, ACARC, ACFSITM, ACITMNBR, ACSTAT, ACUSID, ACUSERID, ACTYPCD )   "+                       
									 	" ( SELECT ACOPCO, ACOPITM, ACOPVND, ACOPVITM, ACBCC, ACPARITM, ACQMLT, ACBRDKWD, ACDESC1, ACDESC2, ACPACK, ACSIZE, ACARN, ACMFGITM, ACGWGT, ACNWGT, ACCWGTFL, ACRWGTFL, ACMPACK, ACTIHI, ACLEN, ACWIDTH, ACHGT, ACTMPZNE, ACGTIN, ACSHLFLF, ACSPP, ACHZMAT, ACCLS, ACPBH, ACARC, ACFSITM, ACITMNBR, ACSTAT, ACUSID, ACUSERID, ACTYPCD  " +
									 	" FROM VBABU.CSACCONV ) ";
		String sql_insertCSACPITMF999999 =  " INSERT INTO CIDB.CSACPITMF ( ACOPCO, ACOPITM, ACOPVND, ACOPVITM, ACBCC, ACPARITM, ACQMLT, ACBRDKWD, ACDESC1, ACDESC2, ACPACK, ACSIZE, ACARN, ACMFGITM, ACGWGT, ACNWGT, ACCWGTFL, ACRWGTFL, ACMPACK, ACTIHI, ACLEN, ACWIDTH, ACHGT, ACTMPZNE, ACGTIN, ACSHLFLF, ACSPP, ACHZMAT, ACCLS, ACPBH, ACARC, ACFSITM, ACITMNBR, ACSTAT, ACUSID, ACUSERID, ACTYPCD)   "+                       
										   " ( SELECT ACOPCO, 999999, ACOPVND, ACOPVITM, ACBCC, ACPARITM, ACQMLT, ACBRDKWD, ACDESC1, ACDESC2, ACPACK, ACSIZE, ACARN, ACMFGITM, ACGWGT, ACNWGT, ACCWGTFL, ACRWGTFL, ACMPACK, ACTIHI, ACLEN, ACWIDTH, ACHGT, ACTMPZNE, ACGTIN, ACSHLFLF, ACSPP, ACHZMAT, ACCLS, ACPBH, ACARC, ACFSITM, 999999, ACSTAT, ACUSID, ACUSERID, ACTYPCD " +
										   " FROM VBABU.CSACCONV WHERE ID = ( SELECT MAX(ID) FROM VBABU.CSACCONV )  ) ";
		
	
	
	
	
	// CDAMVNXF/CDAMCONV conversion sqls
	String sql_syncCDAMCVOV =	" UPDATE vbabu.cvovorg a SET a. OVCVNDID = (SELECT AMCVNDID FROM CIDB.CDAMVNXF WHERE AMOPNBR = a.OVOPCONBR AND   a.OVOVNDID = AMOPVND ) "
			+ "  WHERE a.OVOVNDID  = ( SELECT AMOPVND FROM  CIDB.CDAMVNXF  WHERE AMOPNBR = a.OVOPCONBR  AND a.OVOVNDID = AMOPVND) "
			+ " AND a.OVOPCONBR = 680  ";
		
	String sql_truncateCDAMCONV = " DELETE FROM VBABU.CDAMCONV ";
	/*String sql_insertCDAMCONV =   " INSERT INTO VBABU.CDAMCONV ( AMOPNBR, AMOPVND, AMSUPNBR, AMUSERID, AMCVNDID, AMCHGUSR, AMCHGDATE, AMOPCOID, AMSYNC, AMSUSPFLG )   "+                       
								  " ( SELECT CDAMVNXF.AMOPNBR, CONV_VND.FU_VEN, CDAMVNXF.AMSUPNBR, CDAMVNXF.AMUSERID, CDAMVNXF.AMCVNDID, CDAMVNXF.AMCHGUSR, CDAMVNXF.AMCHGDATE, CDAMVNXF.AMOPCOID, CDAMVNXF.AMSYNC,CDAMVNXF.AMSUSPFLG FROM VBABU.CONV_VND "
								  + " LEFT OUTER JOIN   CIDB.CDAMVNXF ON CO_VEN = AMCVNDID WHERE AMOPNBR = 680 )      ";*/
	//Table VBABU.CONV_VND is Approved vendor list from MDM
	//Table VBABU.CDAMORG is the original copy of CIDB.CDAMVNXF 
	//Table CONV_VND has approved MDM vendor list and it is created using SELECT [FUTURE_ERP_VEND_ID] AS FU_VEN ,[PFG_VEND_NUM] AS CO_VEN FROM [MigrationService_Dev].[REFDATA].[PFG_VENDOR_XREF]
	
	String sql_insertCDAMCONV =   " INSERT INTO VBABU.CDAMCONV ( AMOPNBR, AMOPVND, AMSUPNBR, AMUSERID, AMCVNDID, AMCHGUSR, AMCHGDATE, AMOPCOID, AMSYNC, AMSUSPFLG )   "+                       
			  " ( SELECT AMOPNBR, CONV_VND.FU_VEN, AMSUPNBR, AMUSERID, AMCVNDID, AMCHGUSR, AMCHGDATE, AMOPCOID, AMSYNC,AMSUSPFLG FROM VBABU.CONV_VND "
			  + " LEFT OUTER JOIN   VBABU.CDAMORG ON CO_VEN = AMCVNDID WHERE AMOPNBR = 680 )      ";
	
	String sql_deleteDupsCDAMCONV =  " DELETE  FROM VBABU.CDAMCONV WHERE ID IN (SELECT  MAX(ID) FROM VBABU.CDAMCONV GROUP BY AMOPVND HAVING COUNT(AMOPVND) >1  ) ";
	
	String sql_deleteDupsForAMSUPNBRuniquenessCDAMCONV =  " DELETE  FROM VBABU.CDAMCONV WHERE ID IN (SELECT  MAX(ID) FROM VBABU.CDAMCONV GROUP BY AMSUPNBR HAVING COUNT(AMSUPNBR) >1  ) ";
	
	String sql_deleteDupsForAMCVNDIDuniquenessCDAMCONV =  " DELETE  FROM VBABU.CDAMCONV WHERE ID IN (SELECT  MAX(ID) FROM VBABU.CDAMCONV GROUP BY AMCVNDID HAVING COUNT(AMCVNDID) >1  ) ";
	
	
	String sql_deleteCDAMVNXF     =  " DELETE  FROM CIDB.CDAMVNXF WHERE AMOPNBR = 680 ";
	String sql_insertCDAMVNXF     = " INSERT INTO  CIDB.CDAMVNXF ( AMOPNBR, AMOPVND, AMSUPNBR, AMUSERID, AMCVNDID, AMCHGUSR,   AMCHGDATE, AMOPCOID, AMSYNC, AMSUSPFLG )   "+                       
								 	" (  SELECT AMOPNBR, AMOPVND, AMSUPNBR, AMUSERID, AMCVNDID, AMCHGUSR,   AMCHGDATE, AMOPCOID, AMSYNC, AMSUSPFLG FROM VBABU.CDAMCONV      )     ";
								 	
	
	
	
	
	// CVOVNDF/CVOVCONV conversion sqls
		String sql_truncateCVOVCONV = " DELETE FROM VBABU.CVOVCONV ";
		/*String sql_insertCDAMCONV =   " INSERT INTO VBABU.CDAMCONV ( AMOPNBR, AMOPVND, AMSUPNBR, AMUSERID, AMCVNDID, AMCHGUSR, AMCHGDATE, AMOPCOID, AMSYNC, AMSUSPFLG )   "+                       
									  " ( SELECT CDAMVNXF.AMOPNBR, CONV_VND.FU_VEN, CDAMVNXF.AMSUPNBR, CDAMVNXF.AMUSERID, CDAMVNXF.AMCVNDID, CDAMVNXF.AMCHGUSR, CDAMVNXF.AMCHGDATE, CDAMVNXF.AMOPCOID, CDAMVNXF.AMSYNC,CDAMVNXF.AMSUSPFLG FROM VBABU.CONV_VND "
									  + " LEFT OUTER JOIN   CIDB.CDAMVNXF ON CO_VEN = AMCVNDID WHERE AMOPNBR = 680 )      ";*/
		String sql_insertCVOVCONV =   " INSERT INTO VBABU.CVOVCONV ( OVOVNDID, OVOPCOID, OVINVEXP, OVSTATID, OVOVNAM, OVTAXID, OVDUNNS, OVX99FLG, OVINSEDT, OVHLHRML, OVNPC, OVDSCPER, OVDSCDAYS, OVNETDAYS, OVCVNDID, OVPOTRNID, OVPOFAX, OVEMAIL, OVRTANAM, OVRTALN1, OVRTALN2, OVRTALN3, OVRTCITY, OVRTSTATE, OVRTZIP, OVRTCNAM, OVRTCPH, OVRTCEMIL, OVRTLCFAX, OVWHANAM, OVWHALN1, OVWHALN2, OVWHALN3, OVWHCITY, OVWHSTATE, OVWHZIP, OVWHCNAM, OVWHCPH, OVWHCEMIL, OVWHLCFAX, OVCHGUSR, OVCHGDATE, OVOPCONBR, OVTRM, OVRADDRESS, OVWADDRESS )   "+                       
				  " ( SELECT  CONV_VND.FU_VEN, OVOPCOID, OVINVEXP, OVSTATID, OVOVNAM, OVTAXID, OVDUNNS, OVX99FLG, OVINSEDT, OVHLHRML, OVNPC, OVDSCPER, OVDSCDAYS, OVNETDAYS, OVCVNDID, OVPOTRNID, OVPOFAX, OVEMAIL, OVRTANAM, OVRTALN1, OVRTALN2, OVRTALN3, OVRTCITY, OVRTSTATE, OVRTZIP, OVRTCNAM, OVRTCPH, OVRTCEMIL, OVRTLCFAX, OVWHANAM, OVWHALN1, OVWHALN2, OVWHALN3, OVWHCITY, OVWHSTATE, OVWHZIP, OVWHCNAM, OVWHCPH, OVWHCEMIL, OVWHLCFAX, OVCHGUSR, OVCHGDATE, OVOPCONBR, OVTRM, OVRADDRESS, OVWADDRESS  FROM VBABU.CONV_VND "
				  + " LEFT OUTER JOIN   VBABU.CVOVORG ON CO_VEN = OVCVNDID WHERE OVOPCONBR = 680 )      ";
		
		String sql_deleteDupsCVOVCONV =  " DELETE  FROM VBABU.CVOVCONV WHERE ID IN (SELECT  MAX(ID) FROM VBABU.CVOVCONV GROUP BY OVOVNDID HAVING COUNT(OVOVNDID) >1  ) ";
		
		
		
		
		
		
		String sql_deleteCVOVNDF     =  " DELETE  FROM CIDB.CVOVNDF WHERE OVOPCONBR = 680 ";
		String sql_insertCVOVNDF     = " INSERT INTO  CIDB.CVOVNDF ( OVOVNDID, OVOPCOID, OVINVEXP, OVSTATID, OVOVNAM, OVTAXID, OVDUNNS, OVX99FLG, OVINSEDT, OVHLHRML, OVNPC, OVDSCPER, OVDSCDAYS, OVNETDAYS, OVCVNDID, OVPOTRNID, OVPOFAX, OVEMAIL, OVRTANAM, OVRTALN1, OVRTALN2, OVRTALN3, OVRTCITY, OVRTSTATE, OVRTZIP, OVRTCNAM, OVRTCPH, OVRTCEMIL, OVRTLCFAX, OVWHANAM, OVWHALN1, OVWHALN2, OVWHALN3, OVWHCITY, OVWHSTATE, OVWHZIP, OVWHCNAM, OVWHCPH, OVWHCEMIL, OVWHLCFAX, OVCHGUSR, OVCHGDATE, OVOPCONBR, OVTRM, OVRADDRESS, OVWADDRESS )   "+                       
									 	" (  SELECT OVOVNDID, OVOPCOID, OVINVEXP, OVSTATID, OVOVNAM, OVTAXID, OVDUNNS, OVX99FLG, OVINSEDT, OVHLHRML, OVNPC, OVDSCPER, OVDSCDAYS, OVNETDAYS, OVCVNDID, OVPOTRNID, OVPOFAX, OVEMAIL, OVRTANAM, OVRTALN1, OVRTALN2, OVRTALN3, OVRTCITY, OVRTSTATE, OVRTZIP, OVRTCNAM, OVRTCPH, OVRTCEMIL, OVRTLCFAX, OVWHANAM, OVWHALN1, OVWHALN2, OVWHALN3, OVWHCITY, OVWHSTATE, OVWHZIP, OVWHCNAM, OVWHCPH, OVWHCEMIL, OVWHLCFAX, OVCHGUSR, OVCHGDATE, OVOPCONBR, OVTRM, OVRADDRESS, OVWADDRESS FROM VBABU.CVOVCONV      )     ";
									 	
	
	
	
	
	Connection connection = null;
	Connection connection2 = null;
	
	
	PreparedStatement preparedStatment = null; 	
	PreparedStatement preparedStatment2 = null; 	
	
	ResultSet resultSet = null;
	ResultSet resultSet2 = null;
	
	public void convertCDAKITXF(){
		try{
			int rowCount = 0;
			System.out.println( "Starting CDAKITXF Conversion........................................ "); 
			
			connection = this.getConnection();
			
			preparedStatment = connection.prepareStatement(sql_syncCDAKCSAC);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_syncCDAKCSAC - "+rowCount); 
			
			//truncate CDAKCONV records
			preparedStatment = connection.prepareStatement(sql_truncateCDAKCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_truncateCDAKCONV - "+rowCount); 
			
			//insert CDAKCONV rows from CDAKITXF using CONV_ITM (approved list of IITEMS )
			preparedStatment = connection.prepareStatement(sql_insertCDAKCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCDAKCONV - "+rowCount); 
			
			//Delete duplication records in CDAKCONV. 
			while(rowCount != 0){
				preparedStatment = connection.prepareStatement(sql_deleteDupsCDAKCONV);
				rowCount = preparedStatment.executeUpdate();
				System.out.println( "updated row count  for sql_deleteDupsCDAKCONV - "+rowCount); 
			}
			
			//Delete all 680 rows from CDAKITXF
			preparedStatment = connection.prepareStatement(sql_deleteCDAKITXF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_deleteCDAKITXF - "+rowCount); 
			
			//Copy rows from CDAKCONV to CDAKITXF
			preparedStatment = connection.prepareStatement(sql_insertCDAKITXF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCDAKITXF - "+rowCount);
			
			//Add 999999 row to CDAKITXF
			preparedStatment = connection.prepareStatement(sql_insertCDAKITXF999999);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCDAKITXF999999 - "+rowCount);
			
			if(connection != null)connection.close();
			System.out.println( "Completed CDAKITXF Conversion........................................ "); 
		}
		catch(Exception e ){
			
			System.out.println("Exception - "+e.toString());
		}
		finally{
			try	{
				if(connection != null)connection.close();
				System.out.println("Connection Closed");
			} catch (Exception e) {
				System.out.println("Database Exception...."+e.toString());
			}
		}
	}
	
	
	//------------------------------------------------------------------------------------------------------------------//
	
	public void convertCSACPITMF(){
		try{
			int rowCount = 0;
			System.out.println( "Starting CSACPITMF Conversion........................................ "); 
			//truncate CSACCONV records
			connection = this.getConnection();
			preparedStatment = connection.prepareStatement(sql_truncateCSACCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_truncateCSACCONV - "+rowCount); 
			
			//insert CSACCONV rows from CSACPITMF using CONV_ITM (approved list of VENDORS )
			preparedStatment = connection.prepareStatement(sql_insertCSACCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCSACCONV - "+rowCount); 
			
			//Delete duplication records in CSACCONV. 
			while(rowCount != 0){
				preparedStatment = connection.prepareStatement(sql_deleteDupsCSACCONV);
				rowCount = preparedStatment.executeUpdate();
				System.out.println( "updated row count  for sql_deleteDupsCSACCONV - "+rowCount); 
			}
			
			//Delete all 680 rows from CSACPITMF
			preparedStatment = connection.prepareStatement(sql_deleteCSACPITMF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_deleteCSACPITMF - "+rowCount); 
			
			//Copy rows from CSACCONV to CSACPITMF
			preparedStatment = connection.prepareStatement(sql_insertCSACPITMF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCSACPITMF - "+rowCount);
			
			//Add 999999 row to CSACPITMF
			preparedStatment = connection.prepareStatement(sql_insertCSACPITMF999999);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCSACPITMF999999 - "+rowCount);
			
			if(connection != null)connection.close();
			System.out.println( "Completed CSACPITMF Conversion........................................ "); 
		}
		catch(Exception e ){
			
			System.out.println("Exception - "+e.toString());
		}
		finally{
			try	{
				if(connection != null)connection.close();
				System.out.println("Connection Closed");
			} catch (Exception e) {
				System.out.println("Database Exception...."+e.toString());
			}
		}
	}
	
	
	//------------------------------------------------------------------------------------------------------------------//
	
	
	
	public void convertCDAMVNXF(){
		try{
			int rowCount = 0;
			System.out.println( "Starting CDAMVNXF Conversion........................................ "); 
			
			connection = this.getConnection();
			
			preparedStatment = connection.prepareStatement(sql_syncCDAMCVOV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_syncCDAMCVOV - "+rowCount); 
			
			//truncate CDAKCONV records
			preparedStatment = connection.prepareStatement(sql_truncateCDAMCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_truncateCDAMCONV - "+rowCount); 
			
			//insert CDAMCONV rows from CDAMVNXF using CONV_ITEM (approved list of items )
			preparedStatment = connection.prepareStatement(sql_insertCDAMCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCDAMCONV - "+rowCount); 
			
			//Delete duplication records in CDAMCONV. 

			while(rowCount != 0){
				preparedStatment = connection.prepareStatement(sql_deleteDupsCDAMCONV);
				rowCount = preparedStatment.executeUpdate();
				System.out.println( "updated row count  for sql_deleteDupsCDAMCONV - "+rowCount); 
			}
			
		/*	//sql_deleteDupsForAMSUPNBRuniquenessCDAMCONV
			rowCount = 1;
			while(rowCount != 0){
				preparedStatment = connection.prepareStatement(sql_deleteDupsForAMSUPNBRuniquenessCDAMCONV);
				rowCount = preparedStatment.executeUpdate();
				System.out.println( "updated row count  for sql_deleteDupsForAMSUPNBRuniquenessCDAMCONV - "+rowCount); 
			}*/
			//Delete all 680 rows from CDAMVNXF
			preparedStatment = connection.prepareStatement(sql_deleteCDAMVNXF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_deleteCDAMVNXF - "+rowCount); 
			
			//Copy rows from CDAMCONV to CDAMVNXF
			preparedStatment = connection.prepareStatement(sql_insertCDAMVNXF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCDAMVNXF - "+rowCount);
			
			
			
			if(connection != null)connection.close();
			System.out.println( "Completed CDAMVNXF Conversion........................................ "); 
		}
		catch(Exception e ){
			
			System.out.println("Exception - "+e.toString());
		}
		finally{
			try	{
				if(connection != null)connection.close();
				System.out.println("Connection Closed");
			} catch (Exception e) {
				System.out.println("Database Exception...."+e.toString());
			}
		}
	}
	
	
	//------// CVOVNDF/CVOVCONV conversion------------------------------------------------------------------------------------------------------------//
	
	public void convertCVOVNDF(){
		try{
			int rowCount = 0;
			System.out.println( "Starting CVOVCONV Conversion........................................ "); 
			//truncate CVOVCONV records
			connection = this.getConnection();
			preparedStatment = connection.prepareStatement(sql_truncateCVOVCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_truncateCVOVCONV - "+rowCount); 
			
			//insert CVOVCONV rows from CVOVNDF using CONV_ITEM (approved list of items )
			preparedStatment = connection.prepareStatement(sql_insertCVOVCONV);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCVOVCONV - "+rowCount); 
			
			//Delete duplication records in CVOVCONV. 

			while(rowCount != 0){
				preparedStatment = connection.prepareStatement(sql_deleteDupsCVOVCONV);
				rowCount = preparedStatment.executeUpdate();
				System.out.println( "updated row count  for sql_deleteDupsCVOVCONV - "+rowCount); 
			}
			
		
			//Delete all 680 rows from CVOVNDF
			preparedStatment = connection.prepareStatement(sql_deleteCVOVNDF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_deleteCVOVNDF - "+rowCount); 
			
			//Copy rows from CDAMCONV to CVOVNDF
			preparedStatment = connection.prepareStatement(sql_insertCVOVNDF);
			rowCount = preparedStatment.executeUpdate();
			System.out.println( "updated row count  for sql_insertCVOVNDF - "+rowCount);
			
			
			
			if(connection != null)connection.close();
			System.out.println( "Completed CVOVNDF Conversion........................................ "); 
		}
		catch(Exception e ){
			
			System.out.println("Exception - "+e.toString());
		}
		finally{
			try	{
				if(connection != null)connection.close();
				System.out.println("Connection Closed");
			} catch (Exception e) {
				System.out.println("Database Exception...."+e.toString());
			}
		}
	}
	
	
	
	
	//------------------------------------------------------------------------------------------------------------------//
	
	public Connection getConnection() {
		try {
				Class.forName("com.ibm.as400.access.AS400JDBCDriver");
				connection = DriverManager.getConnection(dbUrl, user,password);
				System.out.println("Got connection");
		} catch (SQLException esql) {
			System.out.println("Database Exception...."+esql.toString());
		}
		catch (Exception e)	{
			System.out.println("Database Exception...."+e.toString());
		}
		return connection;
	}
	
	
	public void closeConnection() {
		try	{
			if(connection != null)connection.close();
			System.out.println("Connection Closed");
		} catch (Exception e) {
			System.out.println("Database Exception...."+e.toString());
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CIDB_ITEM_Converter cidb_item_converter = new  CIDB_ITEM_Converter();
		cidb_item_converter.convertCDAKITXF();
		cidb_item_converter.convertCSACPITMF();
		cidb_item_converter.convertCDAMVNXF();
		cidb_item_converter.convertCVOVNDF();
	}

}
