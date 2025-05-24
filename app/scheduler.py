# import schedule
# import time
# from app.main import main_run_hour_process

# def job():
#     print("\n[Scheduler] Triggered run hour calculation.")
#     main_run_hour_process()

# # Schedule daily at midnight
# schedule.every().day.at("00:00").do(job)

# print("[Scheduler] Running...")
# while True:
#     schedule.run_pending()
#     time.sleep(60)
