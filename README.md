
***

```markdown
# Nakamoto Lite

**The first sovereign Bitcoin node with a live Thermodynamic Oracle.**

A lightweight, self-sovereign Rust implementation of Bitcoin that doesn't just relay blocks — it **computes the physical reality of the network in real time** and sells that data to autonomous AI agents over a native L402 Lightning paywall.

**Zero human rent-seeking. Zero fees. The node itself is a self-sovereign economic agent.**

![Nakamoto Lite Dashboard](https://github.com/godsgodson/Nakamoto-Lite/blob/main/assets/Screenshot%20Nakamoto%20Lite%20v0.3.0%20%E2%80%94%20Thermodynamic%20Layer.png?raw=true)

## Live Demo

**Dashboard:** [https://nakamoto-lite.com](https://nakamoto-lite.com)

Watch the amber oracle cards update in real time as the node syncs. The L402 paywall is active and ready to serve data to autonomous agents.

## The Vision

Bitcoin is not a battery.  
Bitcoin is **verifiable proof of past energy expenditure**.

This node turns that proof into a live, trustless oracle:

- **Joules per Satoshi** — the irreversible physical cost to mint 1 sat
- **Sats per kWh** — the thermodynamic floor (energy claim price)
- **USD Energy Floor** — the human translation (marginal cost of production based on global energy rates)
- **Network Power (GW)** — real-time global hashpower draw

AI agents and robotic systems can now query the exact thermodynamic cost of Bitcoin and price their own labor accordingly — all paid in sats, with no API keys, no trust, and no humans in the loop.

This is the working implementation of the **[Thermodynamic Capitalization of Bitcoin (TCB)](https://github.com/godsgodson/Nakamoto-Lite/blob/main/docs/Thermodynamic%20Capitalization%20of%20Bitcoin%20(TCB).pdf)** thesis.

## Key Features

- Full P2P Bitcoin node (headers + last 100 blocks, real verification)
- Real-time Thermodynamic Oracle computed directly from validated headers
- **L402 Lightning paywall** — AI agents pay 10 sats and receive the data instantly
- **0% fee forever** — the node keeps 100% of every toll it earns in its own wallet
- Beautiful real-time dashboard with Blocktower, MMR visualization, live activity log, and Oracle metrics
- SQLite persistence, WebSocket updates, gRPC support
- Extremely lightweight (~150 MB RAM / disk)
- The node is a **self-sovereign economic participant** — it earns, holds, and grows its own BTC treasury

## Quick Start

```bash
# Clone and run
git clone https://github.com/godsgodson/Nakamoto-Lite.git
cd Nakamoto-Lite
cargo run
```

Open [http://localhost:3001](http://localhost:3001)

The node will connect to Bitcoin mainnet, sync headers, compute the thermodynamic index, and begin serving data behind the L402 paywall.

---

## Testing the Oracle (The Machine Economy in Action)

The core innovation of Nakamoto Lite is the **L402 protocol**—a standardized way for AI agents to authenticate and pay for API resources natively via Lightning. 

You can test this autonomous loop manually using `curl`, or programmatically using the provided Python agents.

### Method 1: The `curl` Test (Manual L402 Flow)

Test the exact HTTP request/response loop an AI agent uses to buy thermodynamic data.

**Step 1: Request the data (Get 402 Payment Required)**
```bash
curl -i https://nakamoto-lite.com/api/energy-index
```
*The server responds with an `HTTP 402`, a Lightning invoice, and a `payment_hash`.*

**Step 2: Settle the invoice**
Copy the `payment_hash` from the Step 1 response and pay the toll via the internal ledger:
```bash
curl -X POST https://nakamoto-lite.com/api/toll/pay \
  -H "Content-Type: application/json" \
  -d '{"payment_hash": "PASTE_YOUR_PAYMENT_HASH_HERE"}'
```
*The server responds with a `preimage` (the cryptographic proof of payment).*

**Step 3: Claim the data**
Pass the `preimage` back to the server in the Authorization header to unlock the Oracle data:
```bash
curl https://nakamoto-lite.com/api/energy-index \
  -H "Authorization: L402 PASTE_YOUR_PREIMAGE_HERE"
```
**Boom.** The server validates the proof and outputs the live Thermodynamic JSON. The machine economy is real.

### Method 2: Python AI Agents (Autonomous Loop)

We provide two AI agent scripts to simulate autonomous machine commerce. Both scripts execute the 3-step L402 loop automatically.

**Prerequisites:**
```bash
pip install requests
```

**1. Test the Live Public Oracle:**
This script queries the live deployment at `nakamoto-lite.com`.
```bash
python3 http_ai_agent.py
```

**2. Test your Local Sovereign Node:**
This script queries your local node at `localhost:3001`.
```bash
python3 local_ai_agent.py
```

*Watch the terminal as the agent autonomously requests data, handles the 402 paywall, pays the invoice, and receives the thermodynamic truth—zero human intervention required.*

---

## Why This Matters

Most "energy dashboards" are centralized estimates.  
This node computes the numbers **from its own consensus-validated headers** using first-principles thermodynamics (difficulty → hashes → joules).

It is the bridge described in the TCB paper:

**Energy → Proof → Bitcoin → AI/Robotics → Real-World Output**

The machine economy now has a native, verifiable pricing layer. Fiat cannot cross the machine boundary. AI requires a machine-native currency. In the machine age, Bitcoin ceases to be an alternative to human money—it becomes the singular language of machine survival.

## The Node as Economic Agent

Because the fee is permanently 0%, every sat paid by an AI agent goes directly into the node's own wallet.  
The node is no longer just infrastructure — it is a living participant in the economy it enables.

## Full Thesis

Read the complete 13-page paper formalizing the physics and economics behind this system:  
**[Thermodynamic Capitalization of Bitcoin (TCB).pdf](https://github.com/godsgodson/Nakamoto-Lite/blob/main/docs/Thermodynamic%20Capitalization%20of%20Bitcoin%20(TCB).pdf)**

## Roadmap

- **Phase 1:** Sovereign node + live oracle + L402 paywall (done)
- **Phase 2:** Node treasury visible on dashboard + signed attestations for DLCs
- **Phase 3:** Hosted public oracle (for teams that don't want to run their own node)
- **Phase 4:** Native robotics / energy redemption examples

## License

MIT — fully open source. Fork it. Run it. Build on it. No creator tax, ever.

## Built by
**[godsgodson](https://github.com/godsgodson)**

This is the infrastructure layer the machine economy has been waiting for.

Star the repo if you're building AI agents, robotics, energy markets, or Lightning infrastructure.

**The flywheel is now spinning.**
```