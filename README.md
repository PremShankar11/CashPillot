# CashPilot - AI-Driven Financial Autopilot for SMBs

CashPilot is an intelligent financial management platform designed for small and medium-sized businesses (SMBs). It combines AI-powered analytics, automated cash flow optimization, and proactive financial decision-making to help businesses maintain healthy cash positions and make data-driven financial decisions.

## 🎯 Overview

CashPilot implements a comprehensive financial autopilot system that:

- **Perceives** financial data through automated receipt/invoice ingestion and transaction monitoring
- **Analyzes** cash flow patterns using Monte Carlo simulations and quantitative modeling
- **Recommends** actionable financial decisions through AI-powered agents
- **Executes** actions via integrated communication channels (email, WhatsApp, etc.)

The platform is built as a full-stack application with a modern React/Next.js frontend and a Python FastAPI backend.

## 📋 Table of Contents

- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Frontend Setup](#frontend-setup)
- [Backend Setup](#backend-setup)
- [Database Configuration](#database-configuration)
- [Running the Application](#running-the-application)
- [Key Features](#key-features)
- [API Documentation](#api-documentation)
- [Architecture](#architecture)
- [Contributing](#contributing)

## 📁 Project Structure

```
CashPilot-main/
├── app/                          # Next.js frontend application
│   ├── components/               # Reusable React components
│   │   ├── ActionInbox.tsx       # Displays pending financial actions
│   │   ├── RadarChart.tsx        # Visualization of financial metrics
│   │   ├── Sidebar.tsx           # Navigation sidebar
│   │   └── SimulationSlider.tsx  # Interactive simulation controls
│   ├── context/                  # React context for state management
│   │   └── SimulationContext.tsx # Global simulation state
│   ├── analytics/                # Analytics dashboard page
│   ├── inbox/                    # Action inbox page
│   ├── ingestion/                # Receipt/invoice ingestion page
│   ├── layout.tsx                # Root layout component
│   ├── page.tsx                  # Main dashboard page
│   ├── globals.css               # Global styles
│   └── mockState.ts              # Mock data for development
├── backend/                      # Python FastAPI backend
│   ├── ai/                       # AI agents and decision engines
│   │   ├── action_generator.py   # Generates financial actions
│   │   ├── board_report.py       # Executive summary generation
│   │   ├── inventory_liquidator.py # Inventory optimization
│   │   ├── negotiation_agent.py  # Vendor negotiation logic
│   │   ├── zombie_detector.py    # Identifies stalled transactions
│   │   └── tools.py              # Shared AI utilities
│   ├── api/                      # API route handlers
│   │   ├── ai_router.py          # AI agent endpoints
│   │   ├── dashboard_router.py   # Dashboard data endpoints
│   │   ├── quant_router.py       # Quantitative analysis endpoints
│   │   ├── simulation_router.py  # Simulation endpoints
│   │   └── router.py             # Main ingestion router
│   ├── core/                     # Core utilities
│   │   └── db.py                 # Database connection management
│   ├── quant/                    # Quantitative analysis engines
│   │   ├── monte_carlo.py        # Monte Carlo cash flow simulations
│   │   ├── optimizer.py          # Cash flow optimization
│   │   ├── phantom_balance.py    # Balance projection
│   │   └── runway_engine.py      # Runway calculation
│   ├── services/                 # Business logic services
│   │   ├── ingestion_pipeline.py # Receipt/invoice processing
│   │   ├── pdf_processor.py      # PDF invoice extraction
│   │   ├── demo_mode.py          # Demo data generation
│   │   └── whatsapp_escalation.py # WhatsApp notifications
│   ├── scripts/                  # Utility and setup scripts
│   │   ├── seed_data.py          # Database seeding
│   │   ├── plaid_simulator.py    # Transaction simulation
│   │   ├── goodwill_scorer.py    # Vendor scoring
│   │   └── run_all.py            # Run all setup scripts
│   ├── main.py                   # FastAPI application entry point
│   ├── README.md                 # Backend documentation
│   └── PDF_PROCESSING.md         # PDF processing guide
├── public/                       # Static assets
├── schema.sql                    # Database schema
├── package.json                  # Frontend dependencies
├── tsconfig.json                 # TypeScript configuration
├── next.config.ts                # Next.js configuration
├── tailwind.config.mjs            # Tailwind CSS configuration
└── .env                          # Environment variables (not in repo)
```

## 🛠 Tech Stack

### Frontend
- **Framework**: Next.js 16.2.1 with React 19.2.4
- **Language**: TypeScript 5
- **Styling**: Tailwind CSS 4
- **Visualization**: Recharts 3.8.0
- **Animation**: Framer Motion 12.38.0
- **Icons**: Lucide React 1.6.0

### Backend
- **Framework**: FastAPI with Uvicorn
- **Language**: Python 3.9+
- **Database**: PostgreSQL
- **AI/ML**: Google Gemini API
- **PDF Processing**: PyMuPDF4LLM
- **String Matching**: RapidFuzz
- **Notifications**: Twilio WhatsApp API

### Database
- **Type**: PostgreSQL
- **Schema**: Relational with UUID primary keys
- **Tables**: Companies, Entities, Transactions, Obligations, Action Logs

## 🚀 Getting Started

### Prerequisites

- **Node.js** 18+ and npm/yarn
- **Python** 3.9+
- **PostgreSQL** 12+
- **Git**

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/cashpilot

# AI/ML
GEMINI_API_KEY=your_gemini_api_key_here

# Twilio (optional, falls back to mock mode)
TWILIO_ACCOUNT_SID=your_twilio_sid
TWILIO_AUTH_TOKEN=your_twilio_auth_token
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
TWILIO_WHATSAPP_TO=whatsapp:+15551234567

# Demo Mode
AUTO_SEED_ON_STARTUP=true
WHATSAPP_MOCK_MODE=true
```

## 📦 Frontend Setup

### Installation

```bash
# Navigate to project root
cd CashPilot-main

# Install dependencies
npm install
```

### Development

```bash
# Start development server
npm run dev
```

The frontend will be available at `http://localhost:3000`

### Build

```bash
# Create production build
npm run build

# Start production server
npm start
```

### Linting

```bash
# Run ESLint
npm run lint
```

## 🐍 Backend Setup

### Installation

```bash
# Navigate to backend directory
cd CashPilot-main/backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install fastapi uvicorn python-multipart psycopg2-binary rapidfuzz google-generativeai python-dotenv pymupdf4llm
```

### Running the Backend

```bash
# From backend directory with venv activated
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

**Swagger UI Documentation**: `http://localhost:8000/docs`

## 🗄 Database Configuration

### Setup PostgreSQL

```bash
# Create database
createdb cashpilot

# Apply schema
psql cashpilot < schema.sql
```

### Database Schema

The application uses the following core tables:

- **companies**: Organization records with balance tracking
- **entities**: Vendors and customers with goodwill scoring
- **transactions**: Cleared financial transactions
- **obligations**: Pending payables and receivables
- **action_logs**: AI-generated actions and their execution status

## ▶️ Running the Application

### Full Stack Setup

1. **Start PostgreSQL** (ensure it's running)

2. **Start Backend**:
   ```bash
   cd backend
   source venv/bin/activate  # or venv\Scripts\activate on Windows
   uvicorn main:app --reload
   ```

3. **Start Frontend** (in a new terminal):
   ```bash
   npm run dev
   ```

4. **Seed Demo Data** (optional):
   ```bash
   cd backend
   python -m scripts.run_all
   ```

### Access the Application

- **Dashboard**: `http://localhost:3000`
- **API Docs**: `http://localhost:8000/docs`

## ✨ Key Features

### 1. Dashboard & Analytics
- Real-time cash position monitoring
- Survival probability visualization using radar charts
- Monte Carlo simulation results
- Interactive simulation controls

### 2. Receipt & Invoice Ingestion
- Drag-and-drop receipt/invoice upload
- Automatic PDF processing with layout preservation
- AI-powered data extraction using Gemini
- Fuzzy matching for vendor reconciliation
- Real-time processing feedback

### 3. Action Inbox
- Pending financial actions from AI agents
- Action status tracking
- Execution history
- Manual action override capability

### 4. Quantitative Analysis
- **Monte Carlo Simulations**: Cash flow probability distributions
- **Runway Calculation**: Days of cash remaining
- **Phantom Balance**: Projected balance scenarios
- **Optimization Engine**: Recommended payment schedules

### 5. AI Agents
- **Action Generator**: Creates financial recommendations
- **Negotiation Agent**: Handles vendor communications
- **Zombie Detector**: Identifies stalled transactions
- **Inventory Liquidator**: Optimization suggestions
- **Board Report**: Executive summaries

### 6. Multi-Channel Execution
- Email notifications
- WhatsApp escalations (with mock mode fallback)
- System alerts
- Execution tracking and logging

## 📡 API Documentation

### Core Endpoints

#### Receipt Ingestion
```
POST /api/ingest/receipt
Content-Type: multipart/form-data

Request:
- file: Image (JPG/PNG) or PDF document

Response:
{
  "message": "Receipt processed successfully",
  "file_type": "image|pdf",
  "parsed_receipt": { ... },
  "reconciliation": { ... }
}
```

#### Dashboard Data
```
GET /api/dashboard/company/{company_id}
GET /api/dashboard/entities/{company_id}
GET /api/dashboard/obligations/{company_id}
```

#### Simulations
```
POST /api/simulation/monte-carlo
GET /api/simulation/runway/{company_id}
```

#### AI Actions
```
GET /api/ai/actions/{company_id}
POST /api/ai/execute-action/{action_id}
```

Full API documentation available at `http://localhost:8000/docs` when backend is running.

## 🏗 Architecture

### Frontend Architecture
- **Component-Based**: Modular React components with TypeScript
- **State Management**: React Context API for global simulation state
- **Styling**: Utility-first CSS with Tailwind
- **Data Fetching**: Server-side and client-side rendering with Next.js

### Backend Architecture
- **API Layer**: FastAPI routers for different domains
- **Service Layer**: Business logic in dedicated service modules
- **AI Layer**: Specialized agents for different decision types
- **Quantitative Layer**: Mathematical models for analysis
- **Data Layer**: PostgreSQL with ORM patterns

### Data Flow
1. **Ingestion**: Receipts/invoices → PDF processor → Gemini extraction → Reconciliation
2. **Analysis**: Transactions + Obligations → Monte Carlo → Runway calculation
3. **Decision**: Analysis results → AI agents → Action generation
4. **Execution**: Actions → Multi-channel delivery → Logging

## 🤝 Contributing

### Code Style
- Frontend: TypeScript with ESLint
- Backend: Python with PEP 8 conventions
- Use meaningful variable and function names
- Add comments for complex logic

### Testing
- Run frontend linter: `npm run lint`
- Test backend endpoints: Use Swagger UI at `/docs`
- Verify database operations with provided scripts

### Submitting Changes
1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Submit a pull request with clear description

## 📝 Additional Documentation

- **Backend Details**: See `backend/README.md`
- **PDF Processing**: See `backend/PDF_PROCESSING.md`
- **Database Schema**: See `schema.sql`

## 🐛 Troubleshooting

### Backend Issues
- **Database connection failed**: Verify PostgreSQL is running and `DATABASE_URL` is correct
- **Gemini API errors**: Check `GEMINI_API_KEY` is valid
- **Port 8000 in use**: Change port with `uvicorn main:app --port 8001`

### Frontend Issues
- **Port 3000 in use**: Change port with `npm run dev -- -p 3001`
- **Module not found**: Run `npm install` again
- **Build errors**: Clear `.next` folder and rebuild

### Database Issues
- **Schema not applied**: Run `psql cashpilot < schema.sql`
- **Connection timeout**: Check PostgreSQL service is running
- **Permission denied**: Verify database user credentials

## 📄 License

This project is proprietary. All rights reserved.

## 📞 Support

For issues, questions, or contributions, please refer to the project documentation or contact the development team.

---

**Last Updated**: May 2026
**Version**: 1.0.0
