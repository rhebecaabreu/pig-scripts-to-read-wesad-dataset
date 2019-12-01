# S2 - MALE 
REGISTER '/home/aluno06/pig-0.17.0/piggybank.jar'
DEFINE POW org.apache.pig.piggybank.evaluation.math.POW;

subject = LOAD '/home/aluno06/teste/WESAD/S3/S3_respiban.txt' USING PigStorage('\t') AS (index:int, ignore:int, signal_ecg:int, signal_eda:int, signal_emg:int, signal_temp:int, xyz1:int, xyz2:int, xyz3:int, signal_resp:int);

values_calculated = FOREACH subject {
	chan_bit = (POW(2,16));
	ecg_value = ((signal_ecg / (chan_bit - 0.5)) * 3);
	eda_value = (((signal_eda / chan_bit) * 3) / 0.12);
	emg_value = ((signal_emg / (chan_bit - 0.5)) * 3);
	resp_value = ((signal_resp / (chan_bit - 0.5)) * 100);
	
  ntc = (signal_temp * 3) / (POW(2,16));
  ntr = (POW(10,4) * ntc) / (3 - ntc);
  ln_ntr = LOG(ntr);
  a0 = (1.12764514 * (POW(10,-3)));
  a1 = (2.34282709 * (POW(10,-4)));
  a2 = (8.77303013 * (POW(10,-8)));
  kel = (1 / (a0 + (a1 * ln_ntr) + (a2 * (POW(ln_ntr,3)))));
  cel = (kel - 273.15);
  temp_value = cel;

	GENERATE 
		ecg_value AS ecg_value,
		eda_value AS eda_value,
		emg_value AS emg_value,
		resp_value AS respiration_value,
		temp_value AS temperature_value;
}

STORE values_calculated INTO 'myoutput3' USING PigStorage ('\t');


teste = FOREACH values_calculated GENERATE AVG(values_calculated.temp_value);
DUMP teste;
# DUMP values_calculated;