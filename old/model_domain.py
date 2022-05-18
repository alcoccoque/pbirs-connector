from pydantic import BaseModel


class User(BaseModel):
    Id: str
    Usernamame: str
    DisplayName: str
    HasFavoriteItems: bool
    MyReportsPath: str

    def get_urn_part(self):
        return "users.{}".format(self.Id)

    def __members(self):
        return (self.Id,)

    def __eq__(self, instance):
        return isinstance(instance, User) and self.__members() == instance.__members()

    def __hash__(self):
        return hash(self.__members())
