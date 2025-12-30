package it.trentuno.zerodue.tre.ph.tasks;

import it.trentuno.zerodue.ConfigNode;
import it.trentuno.zerodue.anag.HL7Constants;
import it.trentuno.zerodue.anag.helpers.CittadinoFullHelper;
import it.trentuno.zerodue.anag.helpers.HL7Helper;
import it.trentuno.zerodue.anag.models.composite.ModelCittadinoFull;
import it.trentuno.zerodue.streamer.Task;
import it.trentuno.zerodue.streamer.TaskException;
import it.trentuno.zerodue.tre.pharmahome.helpers.StringHelper;
import it.trentuno.zerodue.tre.pharmahome.helpers.model.PazientiModelHelper;
import it.trentuno.zerodue.tre.pharmahome.models.ModelCittadinoPharmaHome;
import it.trentuno.zerodue.tre.pharmahome.models.auto.ModelPazienti;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.Functor;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.InvalidLengthException;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbImpegnative;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbMessaggi_anagrafici;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbMessaggi_anagrafici_in;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.datatype.CX;
import ca.uhn.hl7v2.model.v25.message.ADT_A05;
import ca.uhn.hl7v2.model.v25.message.ADT_A39;
import ca.uhn.hl7v2.model.v25.segment.MRG;

public class ImportAnagrafica extends Task {
	
	Logger log = Logger.getLogger(this.getClass());
	
	private boolean initGiro = false;
	
	private StringHelper sh = new StringHelper();
	private HL7Helper hh = new HL7Helper();
	private CittadinoFullHelper ch = new CittadinoFullHelper();

	@Override
	public void exec() throws Exception {
		if(!initGiro){
			Transaction.run(new Functor() {
				@Override
				public void execute(Connection conn) throws Exception {	
					TbMessaggi_anagrafici_in messaggio_in = null;
					Integer last = null;
					boolean another_one;
					do
					{
						another_one = false;
						messaggio_in = null;
						Vector<TbMessaggi_anagrafici_in> list = new TbMessaggi_anagrafici_in(conn).retrieveAll(
								"where " + TbMessaggi_anagrafici_in.COLUMN_fault + " is false " +
									(last!=null ? "and " + TbMessaggi_anagrafici_in.COLUMN_id_messaggio_anagrafico_in + " > " + last + " " : "") +
								"order by " + TbMessaggi_anagrafici_in.COLUMN_id_messaggio_anagrafico_in + " asc " +
								"limit 1");
							if(list!=null && list.size()>0){
								another_one = true;
								try{
									boolean direct_delete = true;
									boolean ok = false;
									messaggio_in = list.get(0);
									last = messaggio_in.getId_messaggio_anagrafico_in();
									log.debug("elaboro il messaggio " + messaggio_in.getId_messaggio_anagrafico_in() + " di tipo " + messaggio_in.getTipo_messaggio());
									if(sh.notEmpty(messaggio_in.getMessaggio())){
										Message msg = hh.parseString(messaggio_in.getMessaggio(), false);									
										if(msg!=null)
											if (msg instanceof ADT_A05){ 	
												direct_delete = false;
												ADT_A05 adt_a05 = (ADT_A05)msg;
												ModelCittadinoFull cittadino = ch.getCittadinoFullFromHL7(adt_a05.getPID(), null, null, null, null, null, false, false);
												if(cittadino!=null && cittadino.getCittadino()!=null && sh.notEmpty(cittadino.getCittadino().getEpid()))
													ok = updataPaziente(cittadino, messaggio_in.getMessaggio(), conn);
											} else 
												if (msg instanceof ADT_A39) {
													direct_delete = false;
													ADT_A39 adt_a39 = (ADT_A39)msg;
													ModelCittadinoFull cittadino = ch.getCittadinoFullFromHL7(adt_a39.getPATIENT(0).getPID(), null, null, null, null, null, false, false);
													if(cittadino!=null && cittadino.getCittadino()!=null && sh.notEmpty(cittadino.getCittadino().getEpid()))
														ok = mergia(cittadino, adt_a39.getPATIENT(0).getMRG(), messaggio_in.getMessaggio(), conn);
												}						
									}	
									
									if(direct_delete || ok)
										if(messaggio_in.delete()){
											conn.commit();
											messaggio_in = null;
										}
								} catch(Exception e){
									if(conn!=null)
										conn.rollback();
									String msg = ExceptionUtils.getStackTrace(e);
									log.error(msg);
								}
								if(messaggio_in!=null){
									//delete
									messaggio_in.setFault(true);
									if(messaggio_in.update())
										conn.commit();
									else
										log.error("NON sono riuscito a updatare il fault del messaggio " + messaggio_in.getId_messaggio_anagrafico_in());
								}
								log.debug("Fine elaborazione del messaggio ");
							}
					} while(another_one);	
					log.debug("Non ci sono più messaggi da processare");
				}
			});
		} else
			initGiro = false;	
	}

	@Override
	public void initialize() throws TaskException {
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
	
	private boolean updataPaziente(ModelCittadinoFull cittadino, String messaggio, Connection conn) throws SQLException, InvalidLengthException{
		boolean value_to_return = true;
		if(cittadino!=null && cittadino.getCittadino()!=null && sh.notEmpty(cittadino.getCittadino().getEpid())){				
			PazientiModelHelper ph = new PazientiModelHelper();
			ModelPazienti paziente_da_updatare = ph.getFrom_epid(cittadino.getCittadino().getEpid(), conn);
			if(paziente_da_updatare!=null){
				value_to_return = false;
				ModelCittadinoPharmaHome cittadino_new = new ModelCittadinoPharmaHome(cittadino, conn);
				ModelPazienti paziente = cittadino_new.toModelPazienti();
				if(ph.update(paziente, conn))	
					value_to_return = true;
				if(value_to_return){
					TbMessaggi_anagrafici messaggio_anagrafico = new TbMessaggi_anagrafici(conn);
					messaggio_anagrafico.setEpid(cittadino.getCittadino().getEpid());
					messaggio_anagrafico.setMessaggio(messaggio);
					messaggio_anagrafico.setTipo_messaggio("U");
					value_to_return = messaggio_anagrafico.insert();
				}
			}	
		}
		return value_to_return;
	}
	
	private boolean mergia(ModelCittadinoFull cittadino, MRG mrg, String messaggio, Connection conn) throws SQLException, InvalidLengthException{
		boolean value_to_return = true;
		if(cittadino!=null && cittadino.getCittadino()!=null && sh.notEmpty(cittadino.getCittadino().getEpid())){
			String epid_to_merge = null;
			for(CX cx : mrg.getMrg1_PriorPatientIdentifierList())
				if(sh.notEmpty(cx.getCx5_IdentifierTypeCode().getValue()) && cx.getCx5_IdentifierTypeCode().getValue().equals(HL7Constants.EPID_CITTADINO_HL7))
					epid_to_merge = cx.getCx1_IDNumber().getValue();
			if(sh.notEmpty(epid_to_merge)){		
				PazientiModelHelper ph = new PazientiModelHelper();
				ModelPazienti paziente_da_mergiare = ph.getFrom_epid(epid_to_merge, conn);
				if(paziente_da_mergiare!=null){
					value_to_return = false;
					paziente_da_mergiare.setConfluito(cittadino.getCittadino().getEpid());
					if(ph.update(paziente_da_mergiare, conn)){
						ModelCittadinoPharmaHome cittadino_new = new ModelCittadinoPharmaHome(cittadino, conn);
						ModelPazienti paziente = cittadino_new.toModelPazienti();
						ModelPazienti paziente_old = ph.getFrom_epid(paziente.getEpid(), conn);
						if(paziente_old!=null){
							if(ph.update(paziente, conn))					
								value_to_return = true;
						} else
							if(ph.insert(paziente, conn))					
								value_to_return = true;
						
						if(value_to_return){
							TbImpegnative impegnativa_conn = new TbImpegnative(conn);
							Vector<TbImpegnative> lista_impegnative = impegnativa_conn.retrieveAll(" where " + TbImpegnative.COLUMN_epid + " = '" + epid_to_merge + "'");
							if(lista_impegnative!=null)
								for(TbImpegnative impegnativa : lista_impegnative)
									if(value_to_return){
										impegnativa.setEpid(cittadino.getCittadino().getEpid());
										value_to_return = impegnativa.update();
									}
						}
						
						if(value_to_return){
							TbMessaggi_anagrafici messaggio_anagrafico = new TbMessaggi_anagrafici(conn);
							messaggio_anagrafico.setEpid(epid_to_merge);
							messaggio_anagrafico.setMessaggio(messaggio);
							messaggio_anagrafico.setTipo_messaggio("M");
							value_to_return = messaggio_anagrafico.insert();
						}
					}
				}
			}
		}
		return value_to_return;
	}

}
