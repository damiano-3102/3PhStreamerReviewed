package it.trentuno.zerodue.tre.ph.tasks;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import it.trentuno.zerodue.ConfigNode;
import it.trentuno.zerodue.streamer.Task;
import it.trentuno.zerodue.streamer.TaskException;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.InvalidLengthException;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.TbFarmaci;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.VsTemp_farmadati_datiprincipaliprodotto;
import it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.VsV_AIR_PROD;

public class ImportFarmaci extends Task {

	Logger log = Logger.getLogger(this.getClass());
	Integer insertFarmaci = 0;
	Integer updateFarmaci = 0;
	Integer scartatiFarmaci = 0;
	Integer giaAggiornatiFarmaci = 0;
	Integer conta = 0;
	Integer contaTotali = 0;
	String where = "";

	private boolean initGiro = false;

	@Override
	public void exec() throws Exception {
		if (!initGiro) {
			try {
				// Runfox - Spezzato in 5 fasi per andare più veloci.
				Vector<VsTemp_farmadati_datiprincipaliprodotto> vFarmaci = new Vector<VsTemp_farmadati_datiprincipaliprodotto>();

				where = " Where " + VsTemp_farmadati_datiprincipaliprodotto.COLUMN_tipologiaprodotto + " = 'F' and "
						+ VsTemp_farmadati_datiprincipaliprodotto.COLUMN_codiceaic + " < '03000000'";
				vFarmaci = new VsTemp_farmadati_datiprincipaliprodotto().retrieveAll(where);
				log.info("FARMADATI: Presenti " + vFarmaci.size() + " farmaci");
				AggiornaDaFarmadati(vFarmaci);

				where = " Where " + VsTemp_farmadati_datiprincipaliprodotto.COLUMN_tipologiaprodotto + " = 'F' and "
						+ VsTemp_farmadati_datiprincipaliprodotto.COLUMN_codiceaic + " Between '03000000' and '03500000'";
				vFarmaci = new VsTemp_farmadati_datiprincipaliprodotto().retrieveAll(where);
				log.info("FARMADATI: Presenti " + vFarmaci.size() + " farmaci");
				AggiornaDaFarmadati(vFarmaci);

				where = " Where " + VsTemp_farmadati_datiprincipaliprodotto.COLUMN_tipologiaprodotto + " = 'F' and "
						+ VsTemp_farmadati_datiprincipaliprodotto.COLUMN_codiceaic + " Between '03500000' and '03900000'";
				vFarmaci = new VsTemp_farmadati_datiprincipaliprodotto().retrieveAll(where);
				log.info("FARMADATI: Presenti " + vFarmaci.size() + " farmaci");
				AggiornaDaFarmadati(vFarmaci);

				where = " Where " + VsTemp_farmadati_datiprincipaliprodotto.COLUMN_tipologiaprodotto + " = 'F' and "
						+ VsTemp_farmadati_datiprincipaliprodotto.COLUMN_codiceaic + " Between '03900000' and '04200000'";
				vFarmaci = new VsTemp_farmadati_datiprincipaliprodotto().retrieveAll(where);
				log.info("FARMADATI: Presenti " + vFarmaci.size() + " farmaci");
				AggiornaDaFarmadati(vFarmaci);

				where = " Where " + VsTemp_farmadati_datiprincipaliprodotto.COLUMN_tipologiaprodotto + " = 'F' and "
						+ VsTemp_farmadati_datiprincipaliprodotto.COLUMN_codiceaic + " > '04200000'";
				vFarmaci = new VsTemp_farmadati_datiprincipaliprodotto().retrieveAll(where);
				log.info("FARMADATI: Presenti " + vFarmaci.size() + " farmaci");
				AggiornaDaFarmadati(vFarmaci);

			} catch (Exception e) {
				String stackTrace = ExceptionUtils.getStackTrace(e);
				log.error(stackTrace);
			}
			System.gc();

			try {
				String where = " Where " + VsV_AIR_PROD.COLUMN_FARMACIESTERI + " is not null";
				Vector<VsV_AIR_PROD> list_farmaci_prod = new VsV_AIR_PROD().retrieveAll(where);
				log.info("INTEGRA: Presenti " + list_farmaci_prod.size() + " farmaci");
				AggiornaDaIntegra(list_farmaci_prod);

			} catch (Exception e) {
				String stackTrace = ExceptionUtils.getStackTrace(e);
				log.error(stackTrace);
			}
			System.gc();

		} else
			initGiro = false;
	}

	private void AggiornaDaIntegra(Vector<VsV_AIR_PROD> list_farmaci_prod) throws SQLException, InvalidLengthException {
		insertFarmaci = 0;
		updateFarmaci = 0;
		scartatiFarmaci = 0;
		giaAggiornatiFarmaci = 0;
		conta = 0;
		contaTotali = 0;
		TbFarmaci farma = new TbFarmaci();
		for (VsV_AIR_PROD farmaco : list_farmaci_prod) {
			try {
				farma = new TbFarmaci();
				contaTotali++;
				if (++conta == 25) {
					conta = 0;
					log.info("Letti " + contaTotali + " records");
				}
				if (farmaco.getFARMACIESTERI().length() < 10) {
					farma.setMinsan(farmaco.getFARMACIESTERI());
					if (farma.retrieveByKey()) {
						if (!equalsIntegra(farma, farmaco)) {
							popolaFarmacoIntegra(farma, farmaco);
							farma.update();
							updateFarmaci++;
						} else {
							giaAggiornatiFarmaci++;
						}

					} else {
						popolaFarmacoIntegra(farma, farmaco);
						farma.insert();
						insertFarmaci++;
					}
				} else {
					scartatiFarmaci++;
					log.warn("Scartato codice Estero: " + farmaco.getFARMACIESTERI());
				}
			} catch (Exception e) {
				String stackTrace = ExceptionUtils.getStackTrace(e);
				log.error(stackTrace);
			}
		}
		log.info("INTEGRA");
		log.info("Letti " + list_farmaci_prod.size() + " records");
		log.info("Inseriti: " + insertFarmaci);
		log.info("Aggiornati: " + updateFarmaci);
		log.info("Scartati: " + scartatiFarmaci);
		log.info("Già Aggiornati: " + giaAggiornatiFarmaci);
	}

	private void AggiornaDaFarmadati(Vector<VsTemp_farmadati_datiprincipaliprodotto> vFarmaci)
			throws SQLException, InvalidLengthException {
		insertFarmaci = 0;
		updateFarmaci = 0;
		scartatiFarmaci = 0;
		giaAggiornatiFarmaci = 0;
		conta = 0;
		contaTotali = 0;
		TbFarmaci farma = new TbFarmaci();
		for (VsTemp_farmadati_datiprincipaliprodotto farmaco : vFarmaci) {
			try {
				farma = new TbFarmaci();
				contaTotali++;
				if (++conta == 1000) {
					conta = 0;
					log.info("Letti " + contaTotali + " records");
				}
				if (farmaco.getCodiceaic().length() < 10) {
					farma.setMinsan(farmaco.getCodiceaic());
					if (farma.retrieveByKey()) {
						if (!equalsFarmadati(farma, farmaco)) {
							popolaFarmacoFarmadati(farma, farmaco);
							farma.update();
							updateFarmaci++;
						} else {
							giaAggiornatiFarmaci++;
						}

					} else {
						popolaFarmacoFarmadati(farma, farmaco);
						farma.insert();
						insertFarmaci++;
					}
				} else {
					scartatiFarmaci++;
					log.warn("Scartato codice AIC: " + farmaco.getCodiceaic());
				}
			} catch (Exception e) {
				String stackTrace = ExceptionUtils.getStackTrace(e);
				log.error(stackTrace);
			}
		}
		log.info("FARMADATI");
		log.info("Letti " + vFarmaci.size() + " records");
		log.info("Inseriti: " + insertFarmaci);
		log.info("Aggiornati: " + updateFarmaci);
		log.info("Scartati: " + scartatiFarmaci);
		log.info("Già Aggiornati: " + giaAggiornatiFarmaci);
	}

	private void popolaFarmacoIntegra(TbFarmaci farma, VsV_AIR_PROD farmaco) {
		farma.setFarmaco(farmaco.getDESCR());
		farma.setEstero(true);
		farma.setClm(farmaco.getCLM());
		farma.setCodice_prodotto(farmaco.getPROD());
		farma.setUnita_misura(farmaco.getUM());
		farma.setFattore_conversione(farmaco.getFATCON());
		farma.setIva(farmaco.getIVA());
		farma.setStato(farmaco.getSTATO());
		farma.setGest_lotti(farmaco.getGEST_LOTTI());
		if (farmaco.getPREZZO_PMP() != null)
			farma.setPrezzo_pmp(farmaco.getPREZZO_PMP().doubleValue());
		farma.setTransito(farmaco.getTRANSITO());
		farma.setProntuario(farmaco.getPRONTUARIO());
		farma.setAutorizzazione(farmaco.getAUTORIZZAZIONE());
		farma.setContratto(farmaco.getCONTRATTO());
		farma.setTipologia_prodotto(farmaco.getTIPO_PRODOTTO());
		farma.setAtc(farmaco.getATC());
		farma.setDescrizione_principio_attivo(farmaco.getPRINCIPIOATTIVO());
		farma.setCodice_generico(farmaco.getCODGENERICO());
		farma.setCnd(farmaco.getCND());
		farma.setDescrizione_cnd(farmaco.getDESCRIZIONECND());
		farma.setFarmaci_esteri(farmaco.getFARMACIESTERI());
		if (farmaco.getDATA_VARIAZIONE() != null)
			farma.setData_variazione((new SimpleDateFormat("yyyyMMdd").format(farmaco.getDATA_VARIAZIONE())));
	}

	private void popolaFarmacoFarmadati(TbFarmaci farma, VsTemp_farmadati_datiprincipaliprodotto farmaco) {
		farma.setFarmaco(farmaco.getProdotto());
		farma.setEstero(false);
		farma.setFattore_conversione(farmaco.getQuantitaconfezione());
		farma.setId_principio_attivo(farmaco.getIdprincipioattivo().setScale(0, RoundingMode.CEILING).intValue());
		// Note: VsTemp_farmadati_datiprincipaliprodotto does not have descrizione_principio_attivo field
		// farma.setDescrizione_principio_attivo(...);
		farma.setClassificazione_atc(farmaco.getClassificazioneatc());
		farma.setId_tipo_prodotto(farmaco.getIdtipoprodotto());
		farma.setTipo_prodotto_descrizione(farmaco.getTipoprodottodescrizione());
		farma.setTipologia_prodotto(farmaco.getTipologiaprodotto());
		farma.setForma_farmaceutica(farmaco.getFormafarmaceutica());
		farma.setId_unita_misura(new BigInteger(farmaco.getIdunitamisuraunitaapplicazione()).intValue());
		farma.setUnita_misura(farmaco.getUnitamisura());
		farma.setUnita_misura_descrizione(farmaco.getDescrizioneunitamisura());
		if (farmaco.getDataimmissionecommercio() != null)
			farma.setData_immissione_commercio(
					new SimpleDateFormat("yyyyMMdd").format(farmaco.getDataimmissionecommercio()));
		farma.setId_classe(farmaco.getIdclasse());
		farma.setClasse_descrizione(new String(farmaco.getDescrizioneclasse()));
		if (farmaco.getDatatiporicetta() != null)
			farma.setData_tipo_ricetta1(new SimpleDateFormat("yyyyMMdd").format(farmaco.getDatatiporicetta()));
		farma.setId_tipo_ricetta1(farmaco.getIdtiporicetta());
		farma.setTipo_ricetta1_descrizione(farmaco.getDescrizionetiporicetta1());
		if (farmaco.getDatatiporicetta2() != null)
			farma.setData_tipo_ricetta2(new SimpleDateFormat("yyyyMMdd").format(farmaco.getDatatiporicetta2()));
		farma.setId_tipo_ricetta2(farmaco.getIdtiporicetta2());
		farma.setTipo_ricetta2_descrizione(farmaco.getDescrizionetiporicetta2());
		farma.setId_regime_ssn(farmaco.getIdregimessn());
		farma.setRegime_sanitario_descrizione(new String(farmaco.getDescrizioneregimesanitario()));
		if (farmaco.getDataprezzo1() != null)
			farma.setData_prezzo1(new SimpleDateFormat("yyyyMMdd").format(farmaco.getDataprezzo1()));
		farma.setId_tipo_prezzo1(farmaco.getIdtipoprezzo1());
		farma.setPrezzo1(farmaco.getPrezzo1());
		farma.setPrezzo1_descrizione(new String(farmaco.getDescrizioneprezzo1()));
		if (farmaco.getDataprezzo2() != null)
			farma.setData_prezzo2(new SimpleDateFormat("yyyyMMdd").format(farmaco.getDataprezzo2()));
		farma.setId_tipo_prezzo2(farmaco.getIdtipoprezzo2());
		farma.setPrezzo2(farmaco.getPrezzo2());
		farma.setPrezzo2_descrizione(new String(farmaco.getDescrizioneprezzo2()));
		if (farmaco.getDatanoteprescrizione1() != null)
			farma.setData_note_prescrizione1(
					new SimpleDateFormat("yyyyMMdd").format(farmaco.getDatanoteprescrizione1()));
		farma.setId_note_prescrizione1(farmaco.getIdnoteprescrizione1());
		farma.setNote_prescrizione1_descrizione(farmaco.getDescrizionenoteprescrizione1());
		if (farmaco.getDatanoteprescrizione2() != null)
			farma.setData_note_prescrizione2(
					new SimpleDateFormat("yyyyMMdd").format(farmaco.getDatanoteprescrizione2()));
		farma.setId_note_prescrizione2(farmaco.getIdnoteprescrizione2());
		farma.setNote_prescrizione2_descrizione(farmaco.getDescrizionenoteprescrizione2());
	}

	private boolean equalsFarmadati(TbFarmaci farma, VsTemp_farmadati_datiprincipaliprodotto farmaco) {
		// Da attivare se si vogliono fare solo aggiornamenti dei records che variano,
		// fare un test sul campo prezzo.
		return false;
	}

	private boolean equalsIntegra(TbFarmaci farma, VsV_AIR_PROD farmaco) {
		// Da attivare se si vogliono fare solo aggiornamenti dei records che variano,
		// fare un test sul campo prezzo.
		return false;
	}

	private boolean equalsWithNull(Object obj1, Object obj2) {
		if (obj1 == null && obj2 != null) {
			return false;
		}
		if (obj1 != null && obj2 == null) {
			return false;
		}
		if (obj1 == null && obj2 == null) {
			return true;
		}
		return obj1.equals(obj2);
	}

	@Override
	public void initialize() throws TaskException {
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.DatabaseAccess(loadDbProperties("database_pharma_dati"));
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.integra.DatabaseAccess(loadDbProperties("database_integra"));
		new it.trentuno.zerodue.tre.pharmahome.tables.auto.TGR.DatabaseAccess(loadDbProperties("database_TGR"));
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

	private boolean notEmpty(String stringa) {
		return stringa != null && stringa.trim().length() > 0;
	}

}
