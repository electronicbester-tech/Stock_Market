#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate Excel and PDF reports from outputs/YYYY-MM-DD/*.csv

Creates an Excel workbook with multiple sheets and a multi-page PDF summary
with key tables and plots.
"""
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_csv_if_exists(p: Path):
    if p.exists():
        try:
            return pd.read_csv(p, encoding='utf-8')
        except Exception:
            return pd.read_csv(p, encoding='cp1256', errors='replace')
    return None


def make_excel(out_dir: Path, date_str: str, excel_path: Path):
    files = {
        'options_candidates': out_dir / f'options_candidates_{date_str}.csv',
        'backtest_detailed': out_dir / f'backtest_options_detailed_{date_str}.csv',
        'backtest_summary': out_dir / f'backtest_options_summary_{date_str}.csv',
        'test_conservative': out_dir / f'test_01_conservative_{date_str}.csv',
        'test_aggressive': out_dir / f'test_01_aggressive_{date_str}.csv',
    }

    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        for sheet, path in files.items():
            df = load_csv_if_exists(path)
            if df is not None:
                try:
                    df.to_excel(writer, sheet_name=sheet[:31], index=False)
                except Exception:
                    # if sheet name duplicate or other error
                    df.to_excel(writer, sheet_name=sheet[:31] + '_', index=False)
    logger.info(f'Wrote Excel report: {excel_path}')


def make_pdf(out_dir: Path, date_str: str, pdf_path: Path, *, company: str = '', project: str = '', support_email: str = ''):
    opt = load_csv_if_exists(out_dir / f'options_candidates_{date_str}.csv')
    det = load_csv_if_exists(out_dir / f'backtest_options_detailed_{date_str}.csv')
    summ = load_csv_if_exists(out_dir / f'backtest_options_summary_{date_str}.csv')

    with PdfPages(pdf_path) as pdf:
        # Title page with header metadata
        fig = plt.figure(figsize=(11.7, 8.3))
        fig.suptitle(f'Options Report — {date_str}', fontsize=20)
        plt.axis('off')
        # add header lines
        header_lines = []
        if project:
            header_lines.append(f'Project: {project}')
        if company:
            header_lines.append(f'Company: {company}')
        if support_email:
            header_lines.append(f'Support: {support_email}')

        if header_lines:
            header_text = '\n'.join(header_lines)
            fig.text(0.02, 0.95, header_text, fontsize=10, va='top')

        pdf.savefig(fig)
        plt.close(fig)

        if opt is not None:
            # Table head
            fig, ax = plt.subplots(figsize=(11.7, 8.3))
            ax.axis('off')
            ax.set_title('Options Candidates (top rows)')
            tbl = opt.head(20)
            # render as text in figure
            ax.text(0, 1, tbl.to_string(index=False), fontfamily='monospace', fontsize=8, va='top')
            pdf.savefig(fig)
            plt.close(fig)

            # Histograms of expected returns
            for col, title in [('mc_call_expected_return_pct', 'MC Call Expected Return %'),
                               ('mc_put_expected_return_pct', 'MC Put Expected Return %'),
                               ('mc_call_expected_return_hp', 'MC Call Expected Return (HP) %')]:
                if col in opt.columns:
                    fig, ax = plt.subplots(figsize=(11.7, 8.3))
                    data = pd.to_numeric(opt[col], errors='coerce').dropna()
                    if not data.empty:
                        ax.hist(data, bins=30)
                        ax.set_title(title)
                        ax.set_xlabel('Return')
                        ax.set_ylabel('Count')
                        pdf.savefig(fig)
                        plt.close(fig)

            # Mean return by symbol (top 10)
            # try mc_call_return fields first, then fallback to mc_call_expected_return_pct
            if 'mc_call_expected_return_pct' in opt.columns:
                try:
                    tmp = pd.to_numeric(opt['mc_call_expected_return_pct'], errors='coerce')
                    avg = opt.assign(mc_call_return=tmp).groupby('symbol')['mc_call_return'].mean().dropna().nlargest(10)
                    fig, ax = plt.subplots(figsize=(11.7, 8.3))
                    avg.plot(kind='bar', ax=ax)
                    ax.set_title('Top 10 symbols by mean MC call expected return')
                    pdf.savefig(fig)
                    plt.close(fig)
                except Exception:
                    pass

        if summ is not None:
            fig, ax = plt.subplots(figsize=(11.7, 8.3))
            ax.axis('off')
            ax.set_title('Backtest Summary (top rows)')
            ax.text(0, 1, summ.head(50).to_string(index=False), fontfamily='monospace', fontsize=8, va='top')
            pdf.savefig(fig)
            plt.close(fig)

    logger.info(f'Wrote PDF report: {pdf_path}')


def main():
    parser = argparse.ArgumentParser(description='Generate Excel and PDF reports from outputs')
    parser.add_argument('--date', '-d', help='Date folder under outputs (YYYY-MM-DD). Default today', default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--out', help='Output folder (defaults to outputs/<date>)', default=None)
    parser.add_argument('--no-pdf', action='store_true', help='Skip PDF generation')
    parser.add_argument('--no-xlsx', action='store_true', help='Skip XLSX generation')
    parser.add_argument('--company', help='Company name for header (default ARNJ)', default='ARNJ')
    parser.add_argument('--project', help='Project name for header (default J.M_Stock_Market)', default='J.M_Stock_Market')
    parser.add_argument('--support-email', help='Support email to include in report header', default='')
    args = parser.parse_args()

    date_str = args.date
    out_dir = Path(args.out) if args.out else Path('outputs') / date_str
    if not out_dir.exists():
        logger.error(f'Output directory not found: {out_dir}')
        return

    timestamp = datetime.now().strftime('%Y-%m-%d')
    excel_path = out_dir / f'report_options_{date_str}.xlsx'
    pdf_path = out_dir / f'report_options_{date_str}.pdf'

    if not args.no_xlsx:
        make_excel(out_dir, date_str, excel_path)
    if not args.no_pdf:
        make_pdf(out_dir, date_str, pdf_path, company=args.company, project=args.project, support_email=args.support_email)


if __name__ == '__main__':
    main()
