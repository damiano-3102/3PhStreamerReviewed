package it.trentuno.zerodue.tre.ph.tasks;

import it.abaco.easy.helper.CalendarHelper;
import it.trentuno.zerodue.ConfigNode;
import it.trentuno.zerodue.streamer.Task;
import it.trentuno.zerodue.streamer.TaskException;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.Functor;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.InvalidLengthException;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbLog_error;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbLog_scaricati_integra;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbScaricabili_integra;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbScaricati_integra;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbTipo_ubicazione;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.Transaction;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.VsCodice_scarico_integra_chiaveriga;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.VsV_da_scaricare_integra;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.TbT_MOVIMENTI_PHARMADOM_IN;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.VsV_AIR_PROD;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;


public class ScaricoFarmaci extends Task {
	
	Logger log = Logger.getLogger(this.getClass());
	
//	String where = "";
	TbTipo_ubicazione ubi = null;
	VsV_da_scaricare_integra v_scaricabile = null;
//	Integer daScaricare = 0;
//	String minsan = "";
//	String msg = "";
//	String chiaveRiga = "";
	
	private boolean initGiro = false;

	@Override
	public void exec() {
		if(!initGiro){
				Vector<TbTipo_ubicazione> ubicazioni = new Vector<TbTipo_ubicazione>();
				String where = " Where " + TbTipo_ubicazione.COLUMN_richiesta_scarico + " is true ";
				try {
					ubicazioni = new TbTipo_ubicazione().retrieveAll(where);
				} catch (SQLException e) {
					log.error("Errore in lettura ubicazioni " + e);
					ubicazioni = new Vector<TbTipo_ubicazione>();
				}
				log.info("Ubicazioni da scaricare: " + ubicazioni.size());

				for (int i=0;i<ubicazioni.size();i++) {
					try {
						ubi = ubicazioni.elementAt(i);
						where = " Where " + VsV_da_scaricare_integra.COLUMN_ubicazione + " = '" + ubi.getCodice() + "' ";
						Vector<VsV_da_scaricare_integra> scaricabili = new Vector<VsV_da_scaricare_integra>();
						scaricabili = new VsV_da_scaricare_integra().retrieveAll(where);
						if(scaricabili!=null)
							for (int j = scaricabili.size()-1; j>=0; j--){
								v_scaricabile = scaricabili.get(j);
								AggiornaFarmaciIntegra();
							}
						ubi.setRichiesta_scarico(false);
						ubi.setData_ultimo_scarico(new CalendarHelper().getCurrentDbDate());
						ubi.update();
					} catch (SQLException e) {
						log.error("Errore in lettura scaricabili " + e);
					} catch (InvalidLengthException e) {
						log.error("Errore in lettura scaricabili " + e);
					}
				}
				log.info("Fine elaborazione");

		} else
			initGiro = false;
	}


	private void AggiornaFarmaciIntegra() { 
		try {
			log.info("Start aggiornamento " + ubi.getDescrizione());
			Transaction.run(new Functor() {
				
				@Override
				public void execute(Connection conn) throws Exception {					
					 String msg = "";		
					 Connection integraConn = null;
					 String minsan = null;
					 TbScaricabili_integra scaricabile = new TbScaricabili_integra(v_scaricabile.getCodice_impegnativa(), v_scaricabile.getMinsan(), conn);
					 try{
						 boolean recordEsistente = true;
						 String vecchio_centro_di_costo_diverso_dal_nuovo = null;
						 String vecchia_ubicazione_diversa_dalla_nuova = null;
						 Integer vecchio_num_diverso_dal_nuovo = null;						
						 minsan = scaricabile.getMinsan();
						 VsCodice_scarico_integra_chiaveriga sic = new VsCodice_scarico_integra_chiaveriga(conn);
						 TbScaricati_integra si = new TbScaricati_integra(conn);
						 //Recupero quanti ne avevo scaricati e quindi quanti ne devo quindi scaricare ora
						 si.setCodice_impegnativa(scaricabile.getCodice_impegnativa());
						 si.setMinsan(minsan);
						 Integer daScaricare;
						 if (si.retrieveByKey()) {
							 if(si.getNum_unita_posologiche()!=null && !si.getNum_unita_posologiche().equals(new Integer(0))){
								 if(si.getCentro_di_costo()!=null && scaricabile.getCentro_di_costo()!=null && 
								 			!si.getCentro_di_costo().equals(scaricabile.getCentro_di_costo())){
							 		vecchio_centro_di_costo_diverso_dal_nuovo = si.getCentro_di_costo();
							 		vecchio_num_diverso_dal_nuovo = si.getNum_unita_posologiche();
								 }
								 if(si.getUbicazione()!=null && scaricabile.getUbicazione()!=null && 
								 			!si.getUbicazione().equals(scaricabile.getUbicazione())){
									 vecchia_ubicazione_diversa_dalla_nuova = si.getUbicazione();
									 vecchio_num_diverso_dal_nuovo = si.getNum_unita_posologiche();
								 }								 
							 }
						 							 		
						 	if(vecchio_num_diverso_dal_nuovo!=null)
						 		daScaricare = scaricabile.getNum_unita_posologiche() - 0;
							else
								daScaricare = scaricabile.getNum_unita_posologiche() - si.getNum_unita_posologiche();
							 recordEsistente = true;
						 } else {
							 daScaricare = scaricabile.getNum_unita_posologiche() - 0;
							 recordEsistente = false;
						 }
						 
						 integraConn = it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.DbGlobal.cm.getConnection();
						 if(vecchio_num_diverso_dal_nuovo!=null && !vecchio_num_diverso_dal_nuovo.equals(new Integer(0))){
							 String chiaveRiga = sic.retrieveAll().firstElement().getChiaveriga();	
							 TbTipo_ubicazione vecchia_ubicazione = null;
							 if(vecchia_ubicazione_diversa_dalla_nuova!=null){
								 Vector<TbTipo_ubicazione> lista_vecchia_ubicazione = new TbTipo_ubicazione(conn).retrieveAll(
										 " where " + TbTipo_ubicazione.COLUMN_codice + " = '" + vecchia_ubicazione_diversa_dalla_nuova + "'");
								 if(lista_vecchia_ubicazione!=null && lista_vecchia_ubicazione.size()>0)
									 vecchia_ubicazione = lista_vecchia_ubicazione.get(0);									 
							 }
							 msg = AggiornaIntegra(conn, integraConn, vecchio_num_diverso_dal_nuovo*-1, minsan, chiaveRiga, 
									 	vecchia_ubicazione!=null ? vecchia_ubicazione : ubi, 
									 	scaricabile.getCodice_impegnativa(), 
									 	vecchio_centro_di_costo_diverso_dal_nuovo!=null ? vecchio_centro_di_costo_diverso_dal_nuovo : scaricabile.getCentro_di_costo(), 
									 	scaricabile.getNote(), scaricabile.getData_ora(), ubi.getUtente_prenotante());
						 }
						 
						 if((msg==null || msg.length()==0) && daScaricare!=null && !daScaricare.equals(new Integer(0))){							 
							 String chiaveRiga = sic.retrieveAll().firstElement().getChiaveriga();
							 msg = AggiornaIntegra(conn, integraConn, daScaricare, minsan, chiaveRiga, ubi, scaricabile.getCodice_impegnativa(), 
									 scaricabile.getCentro_di_costo(), scaricabile.getNote(), scaricabile.getData_ora(), ubi.getUtente_prenotante());							
						 }		
						 
						 //Aggiornare i log di scarico se non si sono verificati errori
						 if (msg==null || msg.length() == 0) {	
							 //Aggiorno la tabella scaricati
							 si.setUbicazione(scaricabile.getUbicazione());
							 si.setNum_unita_posologiche(scaricabile.getNum_unita_posologiche());
							 si.setCentro_di_costo(scaricabile.getCentro_di_costo());
							 si.setNote(scaricabile.getNote());
							 if (recordEsistente){
								 if(!si.update())
									 msg = "Problemi durante l'update nella tabella scaricati";
							 } else 
								 if(!si.insert())
									 msg = "Problemi durante l'insert nella tabella scaricati";	
							 if(msg!=null && msg.length()>0)
								 conn.rollback();
						 }		
						 
						 if((msg==null || msg.length()==0) && !scaricabile.delete())
							 msg = "Problemi durante l'eliminazione dello scaricabile";	
						 
						 if (msg==null || msg.length()==0){							 
							 integraConn.commit();
							 conn.commit();
						 }							 
						 else{
							 integraConn.rollback();
							 conn.rollback();
						 }
						 
					 } catch(Exception e){
						 if(conn!=null)
							 conn.rollback();
						 if(integraConn!=null)
							 integraConn.rollback();
						 msg = ExceptionUtils.getStackTrace(e);
						 log.error(msg);
					 }finally{
						if(integraConn!=null)
							it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.DbGlobal.cm.putConnection(integraConn);
					}
					
					//Scrivere un log error con msg se ci sono stati problemi
					if (msg!=null && msg.length()!=0) {	
						TbLog_error le = new TbLog_error(conn);
						le.setCodice_impegnativa(scaricabile.getCodice_impegnativa());
						le.setMinsan(minsan);
						if (le.retrieveByKey()) {
							le.setError(msg);
							le.update();
						} else {
							le.setError(msg);
							le.insert();
						}
					}									
				}
			});
		} catch (Exception e) {
			log.error("Errore in aggiorna Farmaci Integra: " + e);
		} 		
	}
	
	private boolean insert_log(Connection conn, String codice_impegnativa, String minsan, String ubicazione, Integer unita_posologiche, String centro_di_costo, String note, String utente) throws SQLException, InvalidLengthException{
		 TbLog_scaricati_integra lsi = new TbLog_scaricati_integra(conn);
		 lsi.setCodice_impegnativa(codice_impegnativa);
		 lsi.setMinsan(minsan);
		 lsi.setUbicazione(ubicazione);
		 lsi.setNum_unita_posologiche(unita_posologiche);
		 lsi.setCentro_di_costo(centro_di_costo);
		 lsi.setNote(note);
		 lsi.setUtente_prenotante(utente);
		 return lsi.insert();
	}
	
	private String AggiornaIntegra(Connection conn, Connection integraConn, Integer integra_daScaricare, String integra_minsan, String integra_chiaveRiga,
			TbTipo_ubicazione integra_ubi, String integra_codice_impegnativa, String integra_centro_di_costo, String integra_note, String data_ora, String utente) throws Exception {
		BigDecimal codProd;
		log.info("Minsan: " + integra_minsan + " da scaricare: " + integra_daScaricare + " centro di costo: " + integra_centro_di_costo);
		//Devo recuperare il codice prodotto
		VsV_AIR_PROD ap = new VsV_AIR_PROD(integraConn);		
		String msg = "";
		String where = " Where " + VsV_AIR_PROD.COLUMN_MINSAN + " = '" + integra_minsan + "'";
		Vector<VsV_AIR_PROD> vAp = ap.retrieveAll(where);
		if(vAp==null || vAp.size()==0){
			where = " Where " + VsV_AIR_PROD.COLUMN_FARMACIESTERI + " = '" + integra_minsan + "'";
			vAp = ap.retrieveAll(where);
		}			
		if (vAp.size() > 0 ) {
			codProd = vAp.firstElement().getPROD();
			log.info("Farmaco: " + integra_minsan + " Cod. Prod: " + codProd);
			TbT_MOVIMENTI_PHARMADOM_IN mpi = new TbT_MOVIMENTI_PHARMADOM_IN(integraConn);
			mpi.setMP_RICETTA(integra_codice_impegnativa);
			mpi.setMP_CHIAVERIGA(integra_chiaveRiga);
			mpi.setMP_MAG(integra_ubi.getCodice_magazzino());
			mpi.setMP_CDC(integra_centro_di_costo);
			mpi.setMP_PROD(codProd.toString());
			mpi.setMP_QTA(new BigDecimal(integra_daScaricare));
			java.util.Date now = new java.util.Date();
			mpi.setMP_DATAMOV(new java.sql.Date(new CalendarHelper().parseDate(data_ora, "yyyyMMddHHmmssSSS").getTime()));
			mpi.setMP_DT_INS(new java.sql.Date(now.getTime()));
			mpi.setMP_UT_INS("PHARMAHOME");
			mpi.setMP_NOTE(integra_note!=null && integra_note.length()>mpi.LEN_MP_NOTE ? integra_note.substring(0, mpi.LEN_MP_NOTE-1) : integra_note);
			if (!mpi.insert()) {
				log.error("Movimento Pharmadom: " + integra_minsan + " Non inserito in INTEGRA");
				msg = "Movimento Pharmadom: " + integra_minsan + " Non inserito in INTEGRA";
			} else
				
				if(!insert_log(conn, integra_codice_impegnativa, integra_minsan, integra_ubi.getCodice(), integra_daScaricare, integra_centro_di_costo, integra_note, utente))
					msg = "Errore durante l'inserimento nel log del farmaco : " + integra_minsan;				
		} else {
			log.error("Farmaco: " + integra_minsan + " Non trovato in INTEGRA");
			msg = "Farmaco: " + integra_minsan + " Non trovato in INTEGRA";
		}
		
		return msg;
	}


	@Override
	public void initialize() throws TaskException {
		// una sola volta
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.DatabaseAccess(loadDbProperties("database_pharma_dati"));
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.DatabaseAccess(loadDbProperties("database_integra"));
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

}
