import requests

BASE_URL = "https://nakamoto-lite.com/"

def access_energy_oracle():
    print("🤖 AI Agent: Requesting Thermodynamic Data...")
    
    # 1. Request the data (Will fail with 402)
    res = requests.get(f"{BASE_URL}/api/energy-index")
    
    if res.status_code == 402:
        body = res.json()
        payment_hash = body['payment_hash']
        amount = body.get('amount_sat', 10)
        
        # CHECK IF LDK IS ONLINE (Real Lightning Invoice)
        if 'bolt11_invoice' in body and body['bolt11_invoice'].startswith('lntb'):
            print(f"\n⚡️ LDK ONLINE: Real Lightning Invoice generated!")
            print(f"Invoice: {body['bolt11_invoice']}")
            print("👉 Pay this invoice with a Testnet Lightning wallet (Zeus/Phoenix in Testnet mode)")
            print("Once paid, the wallet will give you the preimage. Use it like this:")
            print(f'curl https://nakamoto-lite.com/api/energy-index -H "Authorization: L402 <preimage_hex>"')
            return
        
        # INTERNAL LEDGER FALLBACK
        print("🤖 AI Agent: 402 Payment Required. Reading invoice...")
        print(f"🤖 AI Agent: Invoice for {amount} sats. Hash: {payment_hash[:16]}...")
        
        # 2. Settle the invoice via internal ledger
        print("🤖 AI Agent: Settling invoice via internal routing node...")
        pay_res = requests.post(f"{BASE_URL}/api/toll/pay", json={"payment_hash": payment_hash})
        
        if pay_res.status_code == 200:
            preimage = pay_res.json()['preimage']
            print(f"🤖 AI Agent: Payment successful! Got cryptographic preimage: {preimage[:16]}...")
            
            # 3. Request the data again with the proof of payment (L402 protocol)
            print("🤖 AI Agent: Requesting data with L402 token...")
            final_res = requests.get(
                f"{BASE_URL}/api/energy-index",
                headers={"Authorization": f"L402 {preimage}"}
            )
            
            if final_res.status_code == 200:
                data = final_res.json()
                print("\n⚡️ THERMODYNAMIC DATA ACQUIRED ⚡️")
                print(f"Joules per Satoshi : {data['joules_per_sat']:.2f}")
                print(f"Sats per kWh       : {data['sat_per_kwh']:.2f}")
                print(f"Network Power (GW) : {data['network_power_gw']:.2f}")
                
                # AI can now price its own labor
                cost_per_kwh_fiat = 0.10 # Assuming $0.10/kWh
                sats_per_dollar = 10000 # Assuming 1 BTC = $100,000
                profitable_rate = (cost_per_kwh_fiat * sats_per_dollar) / data['sat_per_kwh']
                print(f"\n🧠 AI DECISION: If I sell compute for > {profitable_rate:.2f}x the BTC base rate, I am profitable.")
                return
                
    print(f"🤖 AI Agent: Error - {res.status_code} {res.text}")

if __name__ == "__main__":
    access_energy_oracle()