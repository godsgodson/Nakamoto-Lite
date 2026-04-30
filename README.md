Nakamoto Lite

The first sovereign Bitcoin node with a live Thermodynamic Oracle.

A lightweight, self-sovereign Rust implementation of Bitcoin that doesn't just relay blocks — it computes the physical reality of the network in real time and sells that data to autonomous AI agents over a native L402 Lightning paywall.

Zero human rent-seeking. Zero fees. The node itself is a self-sovereign economic agent.

Nakamoto Lite Dashboard
The Vision

Bitcoin is not a battery.
Bitcoin is verifiable proof of past energy expenditure.

This node turns that proof into a live, trustless oracle:

    Joules per Satoshi — the irreversible physical cost to mint 1 sat
    Sats per kWh — the thermodynamic floor (energy claim price)
    Network Power (GW) — real-time global hashpower draw

AI agents and robotic systems can now query the exact thermodynamic cost of Bitcoin and price their own labor accordingly — all paid in sats, with no API keys, no trust, and no humans in the loop.

This is the working implementation of the Thermodynamic Capitalization of Bitcoin (TCB) thesis.
Key Features

    Full P2P Bitcoin node (headers + last 100 blocks, real verification)
    Real-time Thermodynamic Oracle computed directly from validated headers
    L402 Lightning paywall — AI agents pay 10 sats and receive the data instantly
    0% fee forever — the node keeps 100% of every toll it earns in its own wallet
    Beautiful real-time dashboard with Blocktower, MMR visualization, live activity log, and embedded TCB thesis
    SQLite persistence, WebSocket updates, gRPC support
    Extremely lightweight (~150 MB RAM / disk)
    The node is a self-sovereign economic participant — it earns, holds, and grows its own BTC treasury
Quick Start

# Clone and rungit clone https://github.com/godsgodson/Nakamoto-Lite.gitcd Nakamoto-Litecargo run

 

Open http://localhost:3001  

The node will connect to Bitcoin mainnet, sync headers, compute the thermodynamic index, and begin serving data behind the L402 paywall. 
Why This Matters 

Most "energy dashboards" are centralized estimates.
This node computes the numbers from its own consensus-validated headers using first-principles thermodynamics (difficulty → hashes → joules). 

It is the bridge described in the TCB paper: 

Energy → Proof → Bitcoin → AI/Robotics → Real-World Output 

The machine economy now has a native, verifiable pricing layer. 
The Node as Economic Agent 

Because the fee is permanently 0%, every sat paid by an AI agent goes directly into the node's own wallet.
The node is no longer just infrastructure — it is a living participant in the economy it enables. 
Full Thesis 

Read the complete 13-page paper:
Thermodynamic Capitalization of Bitcoin (TCB).pdf  
Roadmap 

     Phase 1: Sovereign node + live oracle (done)
     Phase 2: Node treasury visible on dashboard + signed attestations for DLCs
     Phase 3: Hosted public oracle (for teams that don't want to run their own node)
     Phase 4: Native robotics / energy redemption examples
     

License 

MIT — fully open source. Fork it. Run it. Build on it. No creator tax, ever. 
Built by 

godsgodson 

This is the infrastructure layer the machine economy has been waiting for. 

Star the repo if you're building AI agents, robotics, energy markets, or Lightning infrastructure. 

The flywheel is now spinning. 
