from cryptomodel.notification_type import NOTIFICATION_TYPE

from CryptoCalculatorService.helpers import if_none_raise


class ScheduledJobCreator:

    def add(self):
        pass

    def add_job(self, background_scheduler, user_notification, balance_service):
        if_none_raise(user_notification)
        if_none_raise(background_scheduler)

        if user_notification.notification_type == NOTIFICATION_TYPE.BALANCE.name:
            hours, minutes = self.parse_every(user_notification.check_every)
            if hours == 0 and minutes == 0:
                background_scheduler.add_job(lambda: balance_service.compute_balance(user_id=user_notification.user_id), trigger='date')
            elif hours == 0:
                print(user_notification.user_id)
                background_scheduler.add_job(lambda: balance_service.compute_balance(user_id=user_notification.user_id), trigger='interval',
                                             minutes=minutes, start_date=user_notification.start_date,
                                             end_date= user_notification.end_date)
            elif minutes == 0:
                background_scheduler.add_job(lambda:  balance_service.compute_balance(user_id=user_notification.user_id), trigger='interval', hours=hours, start_date=user_notification.start_date,
                                             end_date=user_notification.end_date)
        elif user_notification.notification_type == NOTIFICATION_TYPE.SYMBOL_VALUE_INCREASE.name:
            raise NotImplementedError(" SYMBOL_VALUE_INCREASE ")
        elif user_notification.notification_type == NOTIFICATION_TYPE.SYMBOL_VALUE_DROP.name:
            raise NotImplementedError("SYMBOL_VALUE_DROP ")

    def parse_every(self, every):
        try:
            return int(every.split(":")[0]), int(every.split(":")[1])
        except:
            return 0, 0
