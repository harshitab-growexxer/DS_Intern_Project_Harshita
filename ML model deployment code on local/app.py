import pandas as pd
import numpy as np
import pickle
import streamlit as st

# loading in the model to predict on the data
pickle_in = open('xgb_model.pkl', 'rb')
model = pickle.load(pickle_in)

scaler_in = open('scaler.pkl', 'rb')
scaler = pickle.load(scaler_in)

# defining the function which will make the prediction using
# the data which the user inputs
def predict_rul(features):
    features_scaled = scaler.transform([features])
    prediction = model.predict(features_scaled)
    return prediction[0]  # Return the prediction without rounding

# this is the main function in which we define our webpage
def main():
    # giving the webpage a title
    st.title("Turbofan Engine RUL Prediction")

    # here we define some of the front end elements of the web page like
    # the font and background color, the padding and the text to be displayed
    html_temp = """
    <div style="background: linear-gradient(to right, #1EAE98, #D8B5FF); padding: 20px; border-radius: 10px; box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);">
    <h1 style="color: #000; text-align: center; font-family: 'Courier New', sans-serif;">Turbofan Engine RUL Prediction</h1>
    </div>
    """
    # this line allows us to display the front end aspects we have
    # defined in the above code
    st.markdown(html_temp, unsafe_allow_html=True)

    # the following lines create text boxes in which the user can enter
    # the data required to make the prediction
    # Create input fields for the new features
    altitude = st.text_input("Altitude (alt)", value=None)
    throttle_angle = st.text_input("Throttle Angle (TRA)", value=None)
    fan_inlet_temp = st.text_input("Fan Inlet Temperature (T2)", value=None)
    LPC_outlet_temp = st.text_input("LPC Outlet Temperature (T24)", value=None)
    HPC_outlet_temp = st.text_input("HPC Outlet Temperature (T30)", value=None)
    LPT_outlet_temp = st.text_input("LPT Outlet Temperature (T50)", value=None)
    fan_inlet_pressure = st.text_input("Fan Inlet Pressure (P2)", value=None)
    bypass_duct_pressure = st.text_input("Bypass Duct Pressure (P15)", value=None)
    HPC_outlet_pressure = st.text_input("HPC Outlet Pressure (P30)", value=None)
    core_speed = st.text_input("Core Speed (Nc)", value=None)
    engine_pressure_ratio = st.text_input("Engine Pressure Ratio (epr)", value=None)
    HPC_outlet_static_pressure = st.text_input("HPC Outlet Static Pressure (Ps30)", value=None)
    fuel_ps30_ratio = st.text_input("Fuel PS30 Ratio (phi)", value=None)
    corrected_core_speed = st.text_input("Corrected Core Speed (NRc)", value=None)
    bypass_ratio = st.text_input("Bypass Ratio (BPR)", value=None)
    bleed_enthalpy = st.text_input("Bleed Enthalpy (htBleed)", value=None)
    demanded_fan_speed = st.text_input("Demanded Fan Speed (Nf_dmd)", value=None)
    demanded_corrected_fan_speed = st.text_input("Demanded Corrected Fan Speed (PCNfR_dmd)", value=None)
    HPT_coolant_bleed = st.text_input("HPT Coolant Bleed (W31)", value=None)
    LPT_coolant_bleed = st.text_input("LPT Coolant Bleed (W32)", value=None)

    # Collecting input features into a list
    features = [altitude, throttle_angle, fan_inlet_temp, LPC_outlet_temp, HPC_outlet_temp,
                LPT_outlet_temp, fan_inlet_pressure, bypass_duct_pressure, HPC_outlet_pressure,
                core_speed, engine_pressure_ratio, HPC_outlet_static_pressure,
                fuel_ps30_ratio, corrected_core_speed, bypass_ratio, bleed_enthalpy, demanded_fan_speed, demanded_corrected_fan_speed,
                HPT_coolant_bleed, LPT_coolant_bleed]

    result = ""

    # Check for missing or invalid values
    if st.button("Predict"):
        if any(feature == "" for feature in features):
            st.error("Please fill in all the input fields.")
        else:
            try:
                features = [float(feature) if feature else None for feature in features]
                prediction = predict_rul(features)
                rounded_prediction = round(prediction)  # Round the prediction to the nearest integer
                st.success('The predicted Remaining Useful Life (RUL) is {}'.format(rounded_prediction))
            except ValueError:
                st.error("Please enter valid numeric values for all fields.")

if __name__ == '__main__':
    main()
