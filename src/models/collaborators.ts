import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

export interface CollaboratorsInterface extends Model {
    id: number;
    appid: number;
    uid: number;
    roles: string;
    created_at: Date;
    updated_at: Date;
}

export const Collaborators = sequelize.define<CollaboratorsInterface>(
    'Collaborators',
    {
        id: {
            type: DataTypes.BIGINT({ length: 20 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        appid: DataTypes.INTEGER({ length: 10 }),
        uid: DataTypes.BIGINT({ length: 20 }),
        roles: DataTypes.STRING,
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'collaborators',
        underscored: true,
        paranoid: true,
    },
);

export async function findCollaboratorsByAppNameAndUid(uid: number, appName: string) {
    const sql =
        'SELECT b.* FROM `apps` as a left join `collaborators` as b  on (a.id = b.appid) where a.name= :appName and b.uid = :uid and a.`deleted_at` IS NULL and b.`deleted_at` IS NULL limit 0,1';
    const data = await sequelize.query(sql, {
        replacements: { appName, uid },
        model: Collaborators,
    });
    return data.pop();
}
