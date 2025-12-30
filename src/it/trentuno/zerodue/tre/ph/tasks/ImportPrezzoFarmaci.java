package it.trentuno.zerodue.tre.ph.tasks;

import it.trentuno.zerodue.ConfigNode;
import it.trentuno.zerodue.streamer.Task;
import it.trentuno.zerodue.streamer.TaskException;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbFarmaci;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbFarmaci_prezzo_storico;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbModalita_distribuzione_farmaci;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.VsV_AIR_PROD;

import java.util.Properties;
import java.util.Vector;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class ImportPrezzoFarmaci extends Task {
	
	Logger log = Logger.getLogger(this.getClass());
	private boolean initGiro = false;

	@Override
	public void exec() throws Exception {
		if(!initGiro){
			log.info("init ImportPrezzoFarmaci");
			String lastMinsanReaded = null;
			do{				
				try{
					Vector<TbFarmaci> listaFarmaci = new TbFarmaci().retrieveAll(
							" where exists " +
							" ( " +
							"	select 1 " +
							"	from " + TbModalita_distribuzione_farmaci.TABLE_NAME + " m " +
							" 	where m." + TbModalita_distribuzione_farmaci.COLUMN_minsan_farmaco + " = " + TbFarmaci.TABLE_NAME + "." + TbFarmaci.COLUMN_minsan + " " +
							" )" +
//							" and " + TbFarmaci.COLUMN_minsan + " = '042640185' " +
							(notEmpty(lastMinsanReaded) ? " and " + TbFarmaci.COLUMN_minsan + " > '" + lastMinsanReaded + "' " : "") +
							" order by " + TbFarmaci.COLUMN_minsan + " asc limit 1");
					lastMinsanReaded = null;
					TbFarmaci farmaco_da_analizzare = null;
					if(listaFarmaci!=null && listaFarmaci.size()>0)
						farmaco_da_analizzare = listaFarmaci.get(0);
					if(farmaco_da_analizzare!=null){
						lastMinsanReaded = farmaco_da_analizzare.getMinsan();		
						String prezzo = null;
						String um = null;
						VsV_AIR_PROD farmaco_integra = null;
						if(farmaco_da_analizzare.getEstero()){
							Vector<VsV_AIR_PROD> list_farmaci_prod = new VsV_AIR_PROD().retrieveAll(" where " + VsV_AIR_PROD.COLUMN_FARMACIESTERI + " = '" + lastMinsanReaded + "'");	
							if(list_farmaci_prod!=null && list_farmaci_prod.size()>0)
								farmaco_integra = list_farmaci_prod.get(0);
						} else{
							Vector<VsV_AIR_PROD> list_farmaci_prod = new VsV_AIR_PROD().retrieveAll(" where " + VsV_AIR_PROD.COLUMN_MINSAN + " = '" + lastMinsanReaded + "'");	
							if(list_farmaci_prod!=null && list_farmaci_prod.size()>0)
								farmaco_integra = list_farmaci_prod.get(0);
						}
						if(farmaco_integra!=null && farmaco_integra.getPREZZO_PMP()!=null){
							prezzo = farmaco_integra.getPREZZO_PMP().toString();
							um = farmaco_integra.getUM();
						}
							
						if(notEmpty(prezzo)){
							boolean insert = false;
							Vector<TbFarmaci_prezzo_storico> list_farmaci_storico = new TbFarmaci_prezzo_storico().retrieveAll(
									" where " + TbFarmaci_prezzo_storico.COLUMN_minsan + " = '" + lastMinsanReaded + "' " +
									" order by " + TbFarmaci_prezzo_storico.COLUMN_data_ora + " desc " +
									" limit 1");
							if(list_farmaci_storico!=null && list_farmaci_storico.size()>0){
								TbFarmaci_prezzo_storico old_farmaco_prezzo = list_farmaci_storico.get(0);
								if(!notEmpty(old_farmaco_prezzo.getPrezzo()) || !old_farmaco_prezzo.getPrezzo().equals(prezzo)){
									log.info("Aggiorno il farmaco " + lastMinsanReaded + " - " + farmaco_da_analizzare.getFarmaco() + " con il prezzo " + prezzo);
									insert = true;
								}									
							} else{
								log.info("Nuovo farmaco " + lastMinsanReaded + " - " + farmaco_da_analizzare.getFarmaco() + " prezzo " + prezzo);
								insert = true;
							}
							if(insert){
								TbFarmaci_prezzo_storico farmaco_da_inserire = new TbFarmaci_prezzo_storico();
								farmaco_da_inserire.setMinsan(lastMinsanReaded);
								farmaco_da_inserire.setPrezzo(prezzo);
								farmaco_da_inserire.setUm(um);
								if(!farmaco_da_inserire.insert())
									log.warn("FARMACO NON INSERITO");
							}
						}						
					}
				} catch(Exception e){
					String stackTrace = ExceptionUtils.getStackTrace(e);
					log.error(stackTrace);
				}				
			} while(notEmpty(lastMinsanReaded));			
			log.info("end ImportPrezzoFarmaci");			
		} else
			initGiro = false;
	}

	@Override
	public void initialize() throws TaskException {
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.DatabaseAccess(loadDbProperties("database_integra"));
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.DatabaseAccess(loadDbProperties("database_ph"));
		initGiro = true;
	}

	@Override
	public void sigShutdown() {
		// TODO Auto-generated method stub
		
	}
	
	private Properties loadDbProperties(String section) {
		return loadDbProperties(getConfigNode().getChildrenSections(section)[0]);
	}

	private Properties loadDbProperties(ConfigNode configNode) {
		Properties toRet = new Properties();
		toRet.put("driver", configNode.getChild("driver"));
		toRet.put("url", configNode.getChild("url"));
		log.info("url database " + configNode.getChild("url"));
		toRet.put("username", configNode.getChild("username"));
		toRet.put("password", configNode.getChild("password"));
		return toRet;
	}
	
	private boolean notEmpty(String stringa){
		return stringa!=null && stringa.trim().length()>0;
	}

}
