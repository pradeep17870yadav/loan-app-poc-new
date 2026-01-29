import os
from decimal import Decimal
from typing import Generator, List, Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Integer, Numeric, String, create_engine
from sqlalchemy.orm import Mapped, Session, declarative_base, mapped_column, sessionmaker

DATABASE_URL = os.getenv("ADMIN_DB_URL", "sqlite:///./admin.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Loan(Base):
    __tablename__ = "loans"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    address: Mapped[str] = mapped_column(String, nullable=False)
    loan_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    opted: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # "pay-mortgage" | "refinance" | None

templates = Jinja2Templates(directory="templates")
app = FastAPI(title="Admin Dashboard")


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def seed_data(db: Session) -> None:
    if db.query(Loan).count() == 0:
        db.add_all(
            [
                Loan(user_id=1, name="Alex Johnson", address="123 Maple St", loan_amount=250000, opted=None),
                Loan(user_id=2, name="Jamie Smith", address="456 Oak Ave", loan_amount=310000, opted=None),
            ]
        )
        db.commit()


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        seed_data(db)


@app.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_db)):
    loans: List[Loan] = db.query(Loan).order_by(Loan.user_id).all()
    return templates.TemplateResponse("admin.html", {"request": request, "loans": loans})


@app.get("/api/loans")
def list_loans(db: Session = Depends(get_db)):
    loans = db.query(Loan).order_by(Loan.user_id).all()
    return [
        {
            "UserID": loan.user_id,
            "Name": loan.name,
            "Address": loan.address,
            "LoanAmount": float(loan.loan_amount),
            "Opted": loan.opted,
        }
        for loan in loans
    ]


@app.post("/opt/{user_id}")
def set_opted(user_id: int, opted: str = Form(...), db: Session = Depends(get_db)):
    if opted not in {"pay-mortgage", "refinance"}:
        raise HTTPException(status_code=400, detail="Invalid option")

    loan = db.query(Loan).filter(Loan.user_id == user_id).first()
    if not loan:
        raise HTTPException(status_code=404, detail="Loan not found")

    loan.opted = opted
    db.commit()
    return RedirectResponse(url="/", status_code=303)


@app.post("/reset/{user_id}")
def reset_opted(user_id: int, db: Session = Depends(get_db)):
    loan = db.query(Loan).filter(Loan.user_id == user_id).first()
    if not loan:
        raise HTTPException(status_code=404, detail="Loan not found")
    loan.opted = None
    db.commit()
    return RedirectResponse(url="/", status_code=303)


@app.post("/opt-by-id")
def set_opted_by_id(
    user_id: int = Form(...),
    opted: str = Form(...),
    name: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    loan_amount: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    if opted not in {"pay-mortgage", "refinance"}:
        raise HTTPException(status_code=400, detail="Invalid option")

    loan = db.query(Loan).filter(Loan.user_id == user_id).first()
    if not loan:
        try:
            amount_val = float(loan_amount) if loan_amount is not None else 0.0
        except ValueError:
            amount_val = 0.0
        loan = Loan(
            user_id=user_id,
            name=name or f"User {user_id}",
            address=address or "Unknown",
            loan_amount=amount_val,
            opted=opted,
        )
        db.add(loan)
    else:
        loan.opted = opted

    db.commit()
    return {"updated": loan.user_id, "opted": loan.opted}
